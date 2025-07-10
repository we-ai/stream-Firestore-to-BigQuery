import fs from 'node:fs/promises';
import path from 'node:path';
import admin from "firebase-admin";
import { bigquery, saveError, saveWarning, cleanupFieldsData, cleanupAndFlattenFields, allTableFieldNameSets, flattenBoxData } from "./utils.js";
import { datasetName, bufferDatasetName, collectionNameArray as tableNameArray, warningLogTableName } from "./settings.js";
import allSchemas from "./tableSchemas.js";
import arraysToBeFlattened from "./arraysToBeFlattened.js";

admin.initializeApp();
const db = admin.firestore();

const ignoredKeys = {
  participants: {
    top: new Set([
      "query",
      "unverifiedSeen",
      "utm_id",
      "utm_source",
      "verifiedSeen",
      "firstSurveyCompletedSeen",
      "569151507",
      "Module2",
      "D_726699695_V2",
      "D_166676176",
      "uid",
      "nullValue",
    ]),
    nested: new Set(["treeJSON", "COMPLETED", "COMPLETED_TS", "sha", "110349197", "543608829", "nullValue"]),
  },
};

/**
 * Sort and de-duplicate fields in the schemas; add common fields to each table schema.
 * @param {object} inputSchemas 
 * @returns 
 */
export const formatSchemas = (inputSchemas) => {
  const commonFields = [
    { name: "docId", type: "STRING" },
    { name: "createdAt", type: "STRING" },
    { name: "updatedAt", type: "STRING" },
  ];
  const commonNames = new Set(commonFields.map((field) => field.name));
  let formattedSchemas = {};

  for (const tableName in inputSchemas) {
    const currSchema = inputSchemas[tableName];
    let visitedNames = new Set();
    let adjustedSchema = [];
    for (const field of currSchema) {
      if (visitedNames.has(field.name) || commonNames.has(field.name)) continue;
      visitedNames.add(field.name);
      adjustedSchema.push(field);
    }

    formattedSchemas[tableName] = [...commonFields, ...adjustedSchema.sort((a, b) => a.name.localeCompare(b.name))];
  }

  return formattedSchemas;
};

export const readJsonFile = async (filePath) => {
  const fileContent = await fs.readFile(filePath);
  return JSON.parse(fileContent);
};

export const writeJsonFile = async (jsonData, outputPath = "") => {
  let filePath = outputPath;
  if (!filePath) {
    filePath = path.join(__dirname, "output.json");
  }

  await fs.writeFile(filePath, JSON.stringify(jsonData, null, 2));
};

export const getTableSchema = async (datasetName, tableName) => {
  const table = bigquery.dataset(datasetName).table(tableName);
  const [metadata] = await table.getMetadata();
  const schema = metadata.schema;
  return schema.fields;
};

export const createTable = async (datasetName, tableName, fieldArray) => {
  const options = {
    schema: fieldArray,
  };

  const [table] = await bigquery.dataset(datasetName).createTable(tableName, options);
  console.log(`Table ${table.id} created.`);
};

export const createAllBufferTables = async () => {
  for (const tableName in allSchemas) {
    const schema = allSchemas[tableName];
    schema.splice(3, 0, { name: "isDeleted", type: "BOOLEAN" }); // Use `isDeleted` field to mark deleted records in buffer tables.
    await createTable(bufferDatasetName, tableName, schema);
  }
};

export const createAllTargetTables = async () => {
  for (const tableName in allSchemas) {
    await createTable(datasetName, tableName, allSchemas[tableName]);
  }
}

export const createLogTables = async () => {
  const schemas = {
    error_log: [
      {
        name: "targetTable",
        type: "STRING",
      },
      {
        name: "docId",
        type: "STRING",
      },
      {
        name: "operation",
        type: "STRING",
      },
      {
        name: "data",
        type: "STRING",
      },
      {
        name: "errorTime",
        type: "STRING",
      },
      {
        name: "errorDetails",
        type: "STRING",
      },
    ],
    warning_log: [
      {
        name: "targetTable",
        type: "STRING",
      },
      {
        name: "docId",
        type: "STRING",
      },
      {
        name: "data",
        type: "STRING",
      },
      {
        name: "warningTime",
        type: "STRING",
      },
      {
        name: "warningDetails",
        type: "STRING",
      },
    ],
  };

  for (const tableName in schemas) {
    await createTable(datasetName, tableName, schemas[tableName]);
  }
};

export const copyTable = async (sourceDataSet, sourceTable, destinationDataSet, destinationTable) => {
  const [job] = await bigquery
    .dataset(sourceDataSet)
    .table(sourceTable)
    .copy(bigquery.dataset(destinationDataSet).table(destinationTable));

  console.log(`Job ${job.id} finished.`);
  console.log(`Table ${sourceTable} copied to ${destinationTable}`);
  const errors = job.status.errors;
  if (errors && errors.length > 0) {
    throw new Error(errors);
  }
};

export const getExtraFieldsFromWarningLogs = async (targetTable) => {
  const deleteExtraFieldsQuery = `DELETE
    FROM
      firestore_stream.warning_logs
    WHERE
      STRUCT(docId,
        warningTime) IN (
      SELECT
        STRUCT(docId,
          warningTime)
      FROM (
        SELECT
          docId,
          warningTime,
          ROW_NUMBER() OVER (PARTITION BY docId ORDER BY warningTime DESC) AS row_num
        FROM
          firestore_stream.warning_logs WHERE targetTable="${targetTable}" AND warningDetails LIKE "Extra%")
      WHERE
        row_num > 1 );`;
  await bigquery.query(deleteExtraFieldsQuery); // Delete redundant rows

  const queryStr = `SELECT warningDetails FROM ${datasetName}.${warningLogTableName} WHERE targetTable="${targetTable}" AND warningDetails LIKE "Extra%"`;
  const [rows] = await bigquery.query(queryStr);
  console.log(`Rows with extra fields: ${rows.length}`);
  const extraFields = new Set();
  const regex = /^Extra.*in\sdata:\s/; // Adjust if pattern changes
  for (const row of rows) {
    if (regex.test(row.warningDetails)) {
      const extraFieldsArray = row.warningDetails.replace(regex, "").split(", ");
      extraFieldsArray.forEach((field) => {
        if (ignoredKeys[targetTable]?.top.has(field)) return;
        extraFields.add(field);
      });
    }
  }

  return [...extraFields].sort();
};

export const getAllRowsFromWarningLogs = async () => {
  const deleteDuplicateRowsQuery = `DELETE
      FROM
        firestore_stream.warning_logs
      WHERE
        STRUCT(docId,
          warningTime) IN (
        SELECT
          STRUCT(docId,
            warningTime)
        FROM (
          SELECT
            docId,
            warningTime,
            ROW_NUMBER() OVER (PARTITION BY docId ORDER BY warningTime DESC) AS row_num
          FROM
            firestore_stream.warning_logs)
        WHERE
          row_num > 1 );`;
  await bigquery.query(deleteDuplicateRowsQuery); // Delete redundant rows
  const queryStr = `SELECT * except(data) FROM ${datasetName}.${warningLogTableName} `;
  const [rows] = await bigquery.query(queryStr);

  return rows;
};

export const changeTableFields = async (datasetName, tableName, addedFields = [], removedFieldNames = []) => {
  const table = bigquery.dataset(datasetName).table(tableName);
  const [metadata] = await table.getMetadata();
  const schema = metadata.schema.fields;

  const existingFileNameSet = new Set(schema.map((field) => field.name));
  const addedFieldNameSet = new Set(addedFields.map((field) => field.name));
  const removedFieldNameSet = new Set(removedFieldNames);
  const count = { addedFields: 0, removedFields: 0 };
  const newFieldNameSet = new Set();
  const mergedFields = [...schema.slice(3), ...addedFields].sort((a, b) => a.name.localeCompare(b.name));
  const visitedFieldNameSet = new Set();
  let newSchema = [];

  for (const field of mergedFields) {
    if (visitedFieldNameSet.has(field.name)) continue;
    visitedFieldNameSet.add(field.name);

    if (removedFieldNameSet.has(field.name)) {
      count.removedFields++;
      continue;
    }

    if (!existingFileNameSet.has(field.name) && addedFieldNameSet.has(field.name)) {
      newFieldNameSet.add(field.name);
      count.addedFields++;
    }

    newSchema.push(field);
  }

  if (count.addedFields === 0 && count.removedFields === 0) {
    console.log(`No changes made to schema for table ${tableName}.`);
    return;
  }

  newSchema = [...schema.slice(0, 3), ...newSchema];

  const tempTableName = `${tableName}_temp`;
  const archiveTableName =
    `${tableName}_archive_` +
    new Date()
      .toISOString()
      .slice(0, -5)
      .replace(/[^0-9]/g, "");
  const existingFieldNamesJoined = newSchema
    .filter((field) => !newFieldNameSet.has(field.name))
    .map((field) => field.name)
    .join(", ");

  const combinedQuery = `
    CREATE TABLE ${datasetName}.${tempTableName} (
        ${newSchema.map((field) => `${field.name} ${field.type}`).join(", ")}
      );
      
      INSERT INTO ${datasetName}.${tempTableName} (${existingFieldNamesJoined})
      SELECT ${existingFieldNamesJoined}
      FROM ${datasetName}.${tableName};

      ALTER TABLE ${datasetName}.${tableName}
      RENAME TO ${archiveTableName};

      ALTER TABLE ${datasetName}.${tempTableName}
      RENAME TO ${tableName};
    
      `;

  await bigquery.query(combinedQuery);
  console.log(`Table ${tableName} updated.`);

  return newSchema;
};

export const updateTableSchema = async (datasetName, tableName, newSchema = []) => {
  if (!Array.isArray(newSchema) || newSchema.length === 0) {
    console.log(`No new schema provided for table ${tableName}.`);
    return;
  }

  const table = bigquery.dataset(datasetName).table(tableName);
  const [metadata] = await table.getMetadata();
  const schema = metadata.schema.fields;

  const existingSchemaFileNames = new Set(schema.map((field) => field.name));
  const newSchemaFieldNames = new Set(newSchema.map((field) => field.name));
  const addedFieldNames = newSchemaFieldNames.difference(existingSchemaFileNames);
  const removedFieldNames = existingSchemaFileNames.difference(newSchemaFieldNames);
  if (addedFieldNames.size === 0 && removedFieldNames.size === 0) {
    console.log(`No changes made to schema for table ${tableName}.`);
    return;
  }

  const tempTableName = `${tableName}_temp`;
  const archiveTableName =
    `${tableName}_archive_` +
    new Date()
      .toISOString()
      .slice(0, -5)
      .replace(/[^0-9]/g, "");
  const existingFieldNamesJoined = newSchema
    .filter((field) => !addedFieldNames.has(field.name))
    .map((field) => field.name)
    .join(", ");

  const combinedQuery = `
    CREATE TABLE ${datasetName}.${tempTableName} (
        ${newSchema.map((field) => `${field.name} ${field.type}`).join(", ")}
      );
      
      INSERT INTO ${datasetName}.${tempTableName} (${existingFieldNamesJoined})
      SELECT ${existingFieldNamesJoined}
      FROM ${datasetName}.${tableName};

      ALTER TABLE ${datasetName}.${tableName}
      RENAME TO ${archiveTableName};

      ALTER TABLE ${datasetName}.${tempTableName}
      RENAME TO ${tableName};

      DROP TABLE ${datasetName}.${archiveTableName};
      `;

  await bigquery.query(combinedQuery);
  console.log(`Table ${tableName} updated.`);
};

export const deleteColumnsFromTable = async (datasetName, tableName, fieldArray) => {
  const queryStr = `ALTER TABLE ${datasetName}.${tableName}
           DROP COLUMN IF EXISTS ${fieldArray.join(", ")}`;

  const [job] = await bigquery.query(queryStr);
  console.log(`Job ${job.id} finished.`);
};

export const changeDataType = async (datasetName, tableName, fieldName, newType) => {
  const queryStr = `ALTER TABLE IF EXISTS ${datasetName}.${tableName}
        ALTER COLUMN [IF EXISTS] column_name SET DATA TYPE ${newType}`;
  const [job] = await bigquery.query(queryStr);
  console.log(`Job ${job.id} finished.`);
};

/**
 *
 * @param {string} tableName Name of the table
 * @param {string} token token value of the row
 * @returns
 */
export const getRowsWithToken = async (tableName, token) => {
  const queryStr = `SELECT * FROM ${datasetName}.${tableName} WHERE token = @token`;
  console.log("Query: ", queryStr);
  const options = {
    query: queryStr,
    params: { token },
  };

  const [rows] = await bigquery.query(options);
  if (rows.length === 0) return [];
  return rows;
};

export const getAllSchemas = async (datasetName) => {
  let tableNames = tableNameArray;
  if (datasetName === "FlatConnect") {
    tableNames = tableNameArray.map((name) => `${name}_JP`);
  }

  let allSchemas = {};
  for (const tableName of tableNames) {
    try {
      let adjustedTableName = tableName;
      if (datasetName === "FlatConnect") {
        adjustedTableName = tableName.replace(/_JP$/, "");
      }
      allSchemas[adjustedTableName] = await getTableSchema(datasetName, tableName);
    } catch (error) {
      console.error(`Error getting schema for ${tableName}:`, error);
      if (error.code === 404) {
        console.log(`Table ${tableName} not found. Continuing...`);
      }

      continue;
    }
  }

  return allSchemas;
};

export const createRowMapFromFirestoreEventData = async (decodedData) => {
  const name = decodedData.value?.name || decodedData.oldValue.name;
  const [tableName, docId] = name.split("/").slice(-2);
  if (decodedData.updateMask && decodedData.updateMask.fieldPaths.length < ignoredKeys[tableName]?.top.size) {
    let ignoredFieldCount = 0;
    for (const fieldName of decodedData.updateMask.fieldPaths) {
      if (ignoredKeys[tableName].top.has(fieldName)) {
        ignoredFieldCount++;
      }
    }

    if (ignoredFieldCount === decodedData.updateMask.fieldPaths.length) {
      return null;
    }
  }

  const [cleanedData, warningMsgArray] = cleanupFieldsData(tableName, decodedData.value.fields);
  const fieldNamesInData = new Set(Object.keys(cleanedData));
  const fieldNamesInSchema = new Set(allSchemas[tableName].map((field) => field.name));
  const extraFieldsInData = fieldNamesInData.difference(fieldNamesInSchema); // Need Node v22 for Set difference
  if (extraFieldsInData.size > 0) {
    warningMsgArray.push(`Extra fields found in data: ${[...extraFieldsInData].join(", ")}`);
  }
  if (warningMsgArray.length > 0) {
    await saveWarning(datasetName, tableName, docId, cleanedData, warningMsgArray.join("; "));
  }

  const createTimeMilliseconds =
    decodedData.value.createTime.seconds * 1000 + Math.round(decodedData.value.createTime.nanos / 1e6);
  const createdAt = new Date(createTimeMilliseconds).toISOString();
  const updateTimeMilliseconds =
    decodedData.value.updateTime.seconds * 1000 + Math.round(decodedData.value.updateTime.nanos / 1e6);
  const updatedAt = new Date(updateTimeMilliseconds).toISOString();

  const rowDataObj = { docId, createdAt, updatedAt, ...cleanedData };

  const rowDataMap = new Map();
  fieldNamesInSchema.forEach((fieldName) => {
    if (rowDataObj[fieldName] !== undefined && rowDataObj[fieldName] !== null) {
      rowDataMap.set(fieldName, rowDataObj[fieldName]);
    }
  });

  return rowDataMap;
};

export const getDocWithMetadataById = async (collectionName, docId) => {
  try {
    const doc = await db.collection(collectionName).doc(docId).get();
    if (!doc.exists) {
      console.log(`No document found with document ID "${docId}"`);
      return null;
    }

    return {
      docId,
      createdAt: doc.createTime.toDate().toISOString(),
      updatedAt: doc.updateTime.toDate().toISOString(),
      ...doc.data(),
    };
  } catch (error) {
    console.error("Error getting document metadata:", error);
  }
};

/**
 * Convert keys to d_1, d_2, etc. for nested objects, and convert integers to strings.
 * @param {object} obj
 * @returns
 */
export const convertDataForBigQuery = (obj) => {
  if (typeof obj !== "object" || obj === null || Array.isArray(obj)) {
    if (typeof obj === "number") {
      obj = obj.toString();
    }
    return obj;
  }

  const newObj = {};
  for (let [key, value] of Object.entries(obj)) {
    const adjustedKey = key.replace(/^(\d)/, "d_$1");
    newObj[adjustedKey] = convertDataForBigQuery(value);
  }
  return newObj;
};

export const getDataWithToken = async (collectionName, token) => {
  const querySnapshot = await db.collection(collectionName).where("token", "==", token).get();
  if (querySnapshot.empty) {
    console.log("No matching documents.");
    return null;
  }
  const doc = querySnapshot.docs[0];
  return {
    docId: doc.id,
    createdAt: doc.createTime.toDate().toISOString(),
    updatedAt: doc.updateTime.toDate().toISOString(),
    ...doc.data(),
  };
};

export const convertKeysAndFlattenForBigQuery = (obj, path = "", parentObj = {}) => {
  let currObj = obj;
  if (typeof obj !== "object" || obj === null || Array.isArray(obj)) {
    if (typeof obj === "number") {
      currObj = obj.toString();
    }

    if (path) {
      parentObj[path] = currObj;
    }
  } else {
    for (let [key, value] of Object.entries(obj)) {
      key = key.replace(/^(\d)/, "d_$1");
      const newPath = path ? `${path}_${key}` : key;
      convertKeysAndFlattenForBigQuery(value, newPath, parentObj);
    }

    return parentObj;
  }
};

export const convertDataToNestedRowMap = (tableSchema, obj) => {
  const rowDataMap = new Map();
  const modifiedObj = convertDataForBigQuery(obj);
  for (const row of tableSchema) {
    if (modifiedObj[row.name] !== undefined && modifiedObj[row.name] !== null) {
      rowDataMap.set(row.name, modifiedObj[row.name]);
    }
  }

  return rowDataMap;
};

export const convertDataToFlattenedRowMap = (tableSchema, obj) => {
  const rowDataMap = new Map();
  const modifiedObj = convertKeysAndFlattenForBigQuery(obj);
  for (const row of tableSchema) {
    if (modifiedObj[row.name] !== undefined && modifiedObj[row.name] !== null) {
      rowDataMap.set(row.name, modifiedObj[row.name]);
    }
  }

  return rowDataMap;
};

export const parseArgs = (args) => {
  let resultObj = { args: [] };
  let prevItem  = { isKey: false, cleanString: "" };

  for (const arg of args) {
    const currCleanString = arg.replace(/^-*/g, "").trim();
    const currIsKey = arg[0] === "-" && currCleanString.length > 0;

    if (prevItem.isKey && currIsKey && prevItem.cleanString) {
      Object.assign(resultObj, { [prevItem.cleanString]: true });
    } else if (prevItem.isKey && !currIsKey && prevItem.cleanString && currCleanString) {
      Object.assign(resultObj, { [prevItem.cleanString]: currCleanString });
    } else if (!prevItem.isKey && !currIsKey && currCleanString) {
      resultObj.args.push(currCleanString);
    }

    prevItem = { isKey: currIsKey, cleanString: currCleanString };
  }

  if (prevItem.isKey && prevItem.cleanString) {
    Object.assign(resultObj, { [prevItem.cleanString]: true });
  }

  return resultObj;
};

export const syncDataFromFirestoreToBigQuery = async (collectionName, token) => {
  const data = await getDataWithToken(collectionName, token);
  if (!data) {
    console.log("No data found with token.");
    return;
  }
  const rowMap = convertDataToFlattenedRowMap(allSchemas[collectionName], data);
  await fs.writeFile(`./${collectionName}_${token}.json`, JSON.stringify(Object.fromEntries(rowMap), null, 2));
  await insertRow(datasetName, collectionName, rowMap);
};

// Fields to be ignored might differ for each table.
const commonTopFields = ["__key__", "__error__", "__has_error__"];
const commonNestedFields = ["__key__"];

const ignoredFields = {
  participants: {
    top: new Set([...commonTopFields,
      "query",
      "unverifiedSeen",
      "undefined",
      "utm_id",
      "utm_source",
      "verifiedSeen",
      "firstSurveyCompletedSeen",
      "d_569151507",
      "Module2",
      "D_726699695_V2",
      "D_166676176",
    ]),
    nested: new Set([
      ...commonNestedFields,
      "treeJSON",
      "COMPLETED",
      "COMPLETED_TS",
      "sha",
      "d_110349197",
      "d_543608829",
    ]),
  },
  bioSurvey_v1: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
    nested: new Set([...commonNestedFields]),
  },
  biospecimen: {
    top: new Set([...commonTopFields]),
    nested: new Set([...commonNestedFields]),
  },
  birthdayCard: {
    top: new Set([...commonTopFields]),
    nested: new Set([...commonNestedFields]),
  },
  boxes: {
    top: new Set([...commonTopFields]),
    nested: new Set([...commonNestedFields]),
  },
  cancerOccurrence: {
    top: new Set([...commonTopFields]),
    nested: new Set([...commonNestedFields]),
  },
  cancerScreeningHistorySurvey: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
    nested: new Set([...commonNestedFields]),
  },
  clinicalBioSurvey_v1: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
    nested: new Set([...commonNestedFields]),
  },
  covid19Survey_v1: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
    nested: new Set([...commonNestedFields]),
  },
  experience2024: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
    nested: new Set([...commonNestedFields]),
  },
  kitAssembly: {
    top: new Set([...commonTopFields]),
    nested: new Set([...commonNestedFields]),
  },
  menstrualSurvey_v1: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
  },
  module1_v1: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
    nested: new Set([...commonNestedFields]),
  },
  module1_v2: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
    nested: new Set([...commonNestedFields]),
  },
  module2_v1: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
    nested: new Set([...commonNestedFields]),
  },
  module2_v2: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
    nested: new Set([...commonNestedFields]),
  },
  module3_v1: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
    nested: new Set([...commonNestedFields]),
  },
  module4_v1: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
    nested: new Set([...commonNestedFields]),
  },
  mouthwash_v1: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
    nested: new Set([...commonNestedFields]),
  },
  notifications: {
    top: new Set([...commonTopFields]),
    nested: new Set([...commonNestedFields]),
  },
  promis_v1: {
    top: new Set([...commonTopFields, "treeJSON", "sha"]),
    nested: new Set([...commonNestedFields]),
  }
}

export const getAndSaveSchemas = async (tierString) => {
  const datasets = ["Connect", "FlatConnect"];
  for (const dataset of datasets) {
    const allSchemaObj = await getAllSchemas(dataset);
    await fs.writeFile(`./temp/schemas/${tierString}_${dataset}_schemas.json`, JSON.stringify(allSchemaObj, null, 2));
  }
  console.log('All schemas saved successfully.');
};

const cleanupFields = (tableName, level, obj) => {
  if (Array.isArray(obj)) {
    return obj
      .map((item) => cleanupFields(tableName, level + 1, item))
      .filter((item) => item !== null)
      .sort((a, b) => a.name.localeCompare(b.name));
  } else if (typeof obj === "object" && obj !== null) {
    if ((level === 1 && ignoredFields[tableName]?.top.has(obj.name)) || (level > 1 && ignoredFields[tableName]?.nested?.has(obj.name)))
      return null;

    const newObj = {};
    for (const [key, value] of Object.entries(obj)) {
      if (key === "description" || (key === "mode" && value === "NULLABLE")) continue;
      let processValue = cleanupFields(tableName, level + 1, value);
      if (processValue !== null) {
        newObj[key] = processValue === "INTEGER" ? "STRING" : processValue;
      }
    }

    return newObj;
  }

  return obj;
};

export const flattenSchema = (schema, parentKey = '', result = []) => {
  for (const field of schema) {
    const newKey = parentKey ? `${parentKey}_${field.name}` : field.name;

    if (field.type === 'RECORD' && field.fields) {
      flattenSchema(field.fields, newKey, result);
    } else {
      result.push({ name: newKey, type: field.type });
    }
  }
  return result;
};

export const cleanupSchemasFile = async (inputJsonFilePath) => {
  const fildDir = path.dirname(inputJsonFilePath);
  const fileName = path.basename(inputJsonFilePath);
  const fileContent = await fs.readFile(inputJsonFilePath);
  let cleanSchemas = JSON.parse(fileContent);
  for (const tableName in cleanSchemas) {
    const cleanedFields = cleanupFields(tableName, 0, cleanSchemas[tableName]);
    cleanSchemas[tableName] = flattenSchema(cleanedFields);
  }

  cleanSchemas = formatSchemas(cleanSchemas);

  const outputFilePath = path.join(
    fildDir,
    path.basename(fileName, path.extname(fileName)) + "_clean" + path.extname(fileName)
  );
  await fs.writeFile(outputFilePath, JSON.stringify(cleanSchemas, null, 2));
  console.log(`Finished "${fileName}". Please check results in file "${outputFilePath}".`);
};

export const compareSchemas = async (baseSchemaPath, otherSchemaPath, outputPath) => {
  const baseFile = await fs.readFile(baseSchemaPath);
  const baseSchemas = JSON.parse(baseFile);
  const otherFile = await fs.readFile(otherSchemaPath);
  const otherSchemas = JSON.parse(otherFile);
  let result = {};
  console.log(`Comparing schemas: ${baseSchemaPath} and ${otherSchemaPath}`);

  for (const tableName of tableNameArray) {
    if (!baseSchemas[tableName] || !otherSchemas[tableName]) {
      console.log(`Schema for ${tableName} not found.`);
      continue;
    }

    const baseFieldsSet = new Set(baseSchemas[tableName].map((field) => field.name));
    const otherFieldsSet = new Set((otherSchemas[tableName].fields || otherSchemas[tableName]).map((field) => field.name));
    const arrayNameSet = new Set(Object.keys(arraysToBeFlattened[tableName] || {}));
    console.log(`${tableName} base schema size`, baseFieldsSet.size);
    console.log(`${tableName} other schema size`, otherFieldsSet.size);
    const additionalFields = {
      additionalFieldsInBase: Array.from(baseFieldsSet.difference(otherFieldsSet)),
      additionalFieldsInOther: Array.from(otherFieldsSet.difference(baseFieldsSet)).filter((field) => !arrayNameSet.has(field)),
    };

    result[tableName] = additionalFields;
  }

  await fs.writeFile(outputPath, JSON.stringify(result, null, 2));
};

/**
 * Simplify the schema by keeping only field names in an array, for each table.
 * @param {string} inputJsonFilePath 
 */
export const simplifySchemaFields = async (inputJsonFilePath) => {
  const fileDir = path.dirname(inputJsonFilePath);
  const fileName = path.basename(inputJsonFilePath);

  const fileData = await fs.readFile(inputJsonFilePath);
  const schemasObj = JSON.parse(fileData);
  for (const tableName in schemasObj) {
    schemasObj[tableName] = schemasObj[tableName].map((field) => field.name);
  }

  const newFilePath = path.join(
    fileDir,
    path.basename(fileName, path.extname(fileName)) + "_fileNameArray" + path.extname(fileName)
  );
  await fs.writeFile(newFilePath, JSON.stringify(schemasObj, null, 2));
  console.log("File processed successfully.");
};

export const formatSchemasFile = async (inputJsonFilePath, outputJsonFilePath) => {
  const fileDir = path.dirname(inputJsonFilePath);
  const fileName = path.basename(inputJsonFilePath);

  const fileData = await fs.readFile(inputJsonFilePath);
  const inputSchemas = JSON.parse(fileData);
  const sortedSchemas = formatSchemas(inputSchemas);


  let outputPath = outputJsonFilePath;
  if (!outputPath) {
    outputPath = path.join(fileDir, `${path.basename(fileName, path.extname(fileName))}_sorted${path.extname(fileName)}`);
  }

  await fs.writeFile(outputPath, JSON.stringify(sortedSchemas, null, 2));
};


export const splitSchemas = async (inputJsonFilePath, folderName) => {
  const fileContent = await fs.readFile(inputJsonFilePath);
  const schemasObj = JSON.parse(fileContent);
  const skippedNameArray = ["birthdayCard", "kitAssembly", "menstrualSurvey_v1", "notifications", "promis_v1"];

  for (const tableName of tableNameArray) {
    if (skippedNameArray.includes(tableName)) continue;
    const currentDate = new Date().toISOString().split('T')[0];
    const newFilePath = path.join(folderName, `${tableName}_${currentDate}.json`);
    const visitedFields = new Set();
    let schema = [];
    for (const field of schemasObj[tableName]) {
      if (visitedFields.has(field.name)) continue;
      schema.push(field);
      visitedFields.add(field.name);
    }

    const sortedSchema = [...schema.slice(0, 3), ...schema.slice(3).sort((a, b) => a.name.localeCompare(b.name))];
    await fs.writeFile(newFilePath, JSON.stringify(sortedSchema, null, 2));
  }
};

export const updateSchemasInBigQuery = async (...inputTableNameArray) =>{
  for (const tableName in allSchemas) {
    if (!inputTableNameArray.includes(tableName)) continue;
    await updateTableSchema(datasetName, tableName, allSchemas[tableName]);
  }
};

export const flattenCidArray = async (outputFilePath) => {
  let schemasResult = { ...allSchemas };
  for (const tableName in arraysToBeFlattened) {
    let currSchema = schemasResult[tableName];
    const currObj = arraysToBeFlattened[tableName];
    for (const path in currObj) {
      currSchema = currSchema.filter((field) => field.name !== path);
      for (const cid of currObj[path]) {
        currSchema.push({ name: `${path}_D_${cid}`, type: "STRING" });
      }
    }

    schemasResult[tableName] = currSchema;
  }

  schemasResult = formatSchemas(schemasResult);
  if (!outputFilePath) {
    outputFilePath = path.join(__dirname, "schemas", "schemas_with_flattened_cid.json");
  }

  await fs.writeFile(outputFilePath, JSON.stringify(schemasResult, null, 2));
};

export const getAndSaveAllRowsFromWarningLogs = async () => {
  const rows = await getAllRowsFromWarningLogs();
  await fs.mkdir("./temp/schemas", { recursive: true });
  await fs.writeFile("./temp/schemas/warnings_records.json", JSON.stringify(rows, null, 2));

  let result = {};
  const regex = /^Extra.*in\sdata:\s/; // Adjust regex if pattern changes
  for (const row of rows) {
    const tableName = row.targetTable;
    if (!result[tableName]) {
      result[tableName] = new Set();
    }

    const extraFieldNameArray = row.warningDetails
      .replace(regex, "")
      .split(", ")
      .map((str) => str.trim());
    for (const fieldName of extraFieldNameArray) {
      if (ignoredKeys[tableName]?.top.has(fieldName)) continue;
      result[tableName].add(fieldName);
    }
  }

  for (const tableName in result){
    result[tableName] = Array.from(result[tableName]).map(fieldName =>({name: fieldName, type: "STRING"}));
  }

  await fs.writeFile("./temp/schemas/warning_fields.json", JSON.stringify(result, null, 2));
};

export const mergeWarningFieldsToSchemas = async () => {
  const warningFieldsFile = await fs.readFile("./temp/schemas/warnings_records.json");
  const warningFields = JSON.parse(warningFieldsFile);

  for (const tableName in warningFields) {
    const currSchema = allSchemas[tableName];
    const currWarningFields = warningFields[tableName];
    allSchemas[tableName] = [...currSchema, ...currWarningFields];
  }

  const allSchemasFormatted = formatSchemas(allSchemas);
  await fs.writeFile("./temp/schemas/schemas_warnings_merged.json", JSON.stringify(allSchemasFormatted, null, 2));
};

export const changeFieldNames = async (datasetName, tableName, oldAndNewdNameObjArray) => {
  const query = `ALTER TABLE ${datasetName}.${tableName}
  ${oldAndNewdNameObjArray.map(({ oldName, newName }) => `RENAME COLUMN ${oldName} TO ${newName}`).join(", ")};`;

  await bigquery.query(query);
  console.log(`Finished changing field names for table ${tableName}.`);
};

export const deleteRow = async (datasetName, tableName, docId) => {
  const query = `
    DELETE FROM ${datasetName}.${tableName}
    WHERE docId = @docId;
  `;

  try {
    await bigquery.query({ query, params: { docId } });
  } catch (error) {
    await saveError(datasetName, tableName, docId, "DELETE", null, error);
  }
};

export const createRowMap = async (datasetName, tableName, docId, decodedData) => {
  if (!allSchemas?.[tableName]) {
    return null;
  }

  if (decodedData.updateMask && decodedData.updateMask.fieldPaths.length < ignoredKeys[tableName]?.top.size) {
    let ignoredFieldCount = 0;
    for (const fieldName of decodedData.updateMask.fieldPaths) {
      if (ignoredKeys[tableName].top.has(fieldName)) {
        ignoredFieldCount++;
      }
    }

    if (ignoredFieldCount === decodedData.updateMask.fieldPaths.length) {
      return null;
    }
  }

  const [flattenedData, warningMsgArray] = cleanupAndFlattenFields(tableName, decodedData.value.fields);
  const fieldNamesInData = new Set(Object.keys(flattenedData));
  const fieldNamesInSchema = allTableFieldNameSets[tableName];
  const extraFieldsInData = fieldNamesInData.difference(fieldNamesInSchema); // Need Node v22 for Set difference
  if (extraFieldsInData.size > 0) {
    warningMsgArray.push(
      `Extra ${extraFieldsInData.size} field(s) found in data: ${[...extraFieldsInData].join(", ")}`
    );
  }
  if (warningMsgArray.length > 0) {
    await saveWarning(datasetName, tableName, docId, flattenedData, warningMsgArray.sort().join("; "));
  }

  const createTimeMilliseconds =
    decodedData.value.createTime.seconds * 1000 + Math.round(decodedData.value.createTime.nanos / 1e6);
  const createdAt = new Date(createTimeMilliseconds).toISOString();
  const updateTimeMilliseconds =
    decodedData.value.updateTime.seconds * 1000 + Math.round(decodedData.value.updateTime.nanos / 1e6);
  const updatedAt = new Date(updateTimeMilliseconds).toISOString();

  const rowObj = { docId, createdAt, updatedAt, ...flattenedData };
  const rowMap = new Map();
  fieldNamesInSchema.forEach((fieldName) => {
    if (rowObj[fieldName] !== undefined && rowObj[fieldName] !== null) {
      rowMap.set(fieldName, rowObj[fieldName]);
    }
  });

  return rowMap;
};

const checkRowExists = async (datasetName, tableName, docId) => {
  const countQuery = `
    SELECT COUNT(*) as count
    FROM ${datasetName}.${tableName}
    WHERE docId = @docId;
    `;

  const [rows] = await bigquery.query({ query: countQuery, params: { docId } });
  const count = rows[0].count;
  if (count > 1) {
    const deleteQuery = `
      DELETE FROM ${datasetName}.${tableName}
      WHERE docId = @docId AND updatedAt NOT IN (
        SELECT MAX(updatedAt) 
        FROM ${datasetName}.${tableName} 
        WHERE docId = @docId );
        `;
    await bigquery.query({ query: deleteQuery, params: { docId } });
  }

  return count > 0;
};

export const insertRow = async (datasetName, tableName, docId, rowMap) => {
  const columnNameArray = Array.from(rowMap.keys());
  const query = `
    INSERT INTO ${datasetName}.${tableName} (${columnNameArray.join(", ")})
    VALUES (${columnNameArray.map((columnName) => `@${columnName}`).join(", ")});
  `;

  try {
    await bigquery.query({ query, params: Object.fromEntries(rowMap) });
  } catch (error) {
    await saveError(datasetName, tableName, docId, "INSERT", rowMap, error);
  }
};


export const bufferUpdates = async (tableName, docId, decodedData) => {
  if (!allSchemas?.[tableName]) {
    console.log(`Schema not found for table "${tableName}".`);
    return;
  }

  if (decodedData.updateMask && decodedData.updateMask.fieldPaths.length < ignoredKeys[tableName]?.top.size) {
    let ignoredFieldCount = 0;
    for (const fieldName of decodedData.updateMask.fieldPaths) {
      if (ignoredKeys[tableName].top.has(fieldName)) {
        ignoredFieldCount++;
      }
    }

    if (ignoredFieldCount === decodedData.updateMask.fieldPaths.length) {
      console.log(`Updated fields are ignored for doc "${docId}" from collection "${tableName}".`);
      return;
    }
  }

  const createTimeMilliseconds =
    decodedData.value.createTime.seconds * 1000 + Math.round(decodedData.value.createTime.nanos / 1e6);
  const createdAt = new Date(createTimeMilliseconds).toISOString();
  const updateTimeMilliseconds =
    decodedData.value.updateTime.seconds * 1000 + Math.round(decodedData.value.updateTime.nanos / 1e6);
  const updatedAt = new Date(updateTimeMilliseconds).toISOString();

  const [fieldsData] = cleanupFieldsData(tableName, decodedData.value.fields);
  const row = { targetTable: tableName, docId, createdAt, updatedAt, fieldsData: JSON.stringify(fieldsData) };
  try {
    await bigquery.dataset(bufferDatasetName).table(tableName).insert(row);
  } catch (error) {
    await saveError(bufferDatasetName, tableName, docId, "UPDATE", fieldsData, error);
  }
};

export const handleTableUpdates = async (datasetName, tableName, docId, decodedData) => {
  const rowMap = await createRowMap(datasetName, tableName, docId, decodedData);
  if (!rowMap) {
    return;
  }

  if (!decodedData.oldValue) {
    await insertRow(datasetName, tableName, docId, rowMap);
  }

  let rowExists = false;
  try {
    rowExists = await checkRowExists(datasetName, tableName, docId);
  } catch (error) {
    await saveError(datasetName, tableName, docId, "CHECK_ROW", null, error);
    return;
  }

  if (!rowExists) {
    await insertRow(datasetName, tableName, docId, rowMap);
    return;
  }

  const query = `
    UPDATE ${datasetName}.${tableName}
    SET ${Array.from(rowMap.keys())
      .map((key) => `${key} = @${key}`)
      .join(", ")}
    WHERE docId = @docId AND updatedAt IN (
      SELECT MAX(updatedAt) 
      FROM ${datasetName}.${tableName} 
      WHERE docId = @docId );
      `;

  try {
    await bigquery.query({ query, params: Object.fromEntries(rowMap) });
  } catch (error) {
    await saveError(datasetName, tableName, docId, "UPDATE", rowMap, error);
  }
};

export const processBoxData = async (datasetName, tableName, docId, decodedData) => {
  if (!allSchemas?.[tableName]) {
    return null;
  }

  if (decodedData.updateMask && decodedData.updateMask.fieldPaths.length < ignoredKeys[tableName]?.top.size) {
    let ignoredFieldCount = 0;
    for (const fieldName of decodedData.updateMask.fieldPaths) {
      if (ignoredKeys[tableName].top.has(fieldName)) {
        ignoredFieldCount++;
      }
    }

    if (ignoredFieldCount === decodedData.updateMask.fieldPaths.length) {
      return null;
    }
  }

  const [boxData, warningMsgArray] = cleanupFieldsData(tableName, decodedData.value.fields);
  if (warningMsgArray.length > 0) {
    await saveWarning(datasetName, tableName, docId, boxData, warningMsgArray.sort().join("; "));
  }

  const { boxId, siteCid, tubeIdSet, rowObjArray } = flattenBoxData(boxData);
  if (rowObjArray.length === 0) {
    return null;
  }

  const createTimeMilliseconds =
    decodedData.value.createTime.seconds * 1000 + Math.round(decodedData.value.createTime.nanos / 1e6);
  const createdAt = new Date(createTimeMilliseconds).toISOString();
  const updateTimeMilliseconds =
    decodedData.value.updateTime.seconds * 1000 + Math.round(decodedData.value.updateTime.nanos / 1e6);
  const updatedAt = new Date(updateTimeMilliseconds).toISOString();

  const fieldNameSet = allTableFieldNameSets[tableName];
  const rowMapArray = [];
  for (const rowObj of rowObjArray) {
    const adjustedRowObj = { docId, createdAt, updatedAt, ...rowObj };
    const rowMap = new Map();
    for (const fieldName of fieldNameSet) {
      if (adjustedRowObj[fieldName] !== undefined && adjustedRowObj[fieldName] !== null) {
        rowMap.set(fieldName, adjustedRowObj[fieldName]);
      }
    }
    rowMapArray.push(rowMap);
  }

  return { rowMapArray, tubeIdSet, boxId, siteCid };
};

export const insertBoxRows = async (datasetName, tableName, docId, rowMapArray) => {
  const columnNameArray = Array.from(rowMapArray[0].keys());
  const query = `
    INSERT INTO ${datasetName}.${tableName} (${columnNameArray.join(", ")})
    VALUES (${columnNameArray.map((columnName) => `@${columnName}`).join(", ")});
  `;

  for (const rowMap of rowMapArray) {
    try {
      await bigquery.query({ query, params: Object.fromEntries(rowMap) });
    } catch (error) {
      await saveError(datasetName, tableName, docId, "INSERT", rowMap, error);
    }
  }
};

export const handleBoxesTableUpdates = async (datasetName, tableName, docId, decodedData) => {
  const resultData = await processBoxData(datasetName, tableName, docId, decodedData);
  if (!resultData || !resultData.rowMapArray || resultData.rowMapArray.length === 0) {
    return;
  }

  const { rowMapArray, tubeIdSet, boxId, siteCid } = resultData;
  if (!decodedData.oldValue) {
    await insertBoxRows(datasetName, tableName, docId, rowMapArray);
    console.log(`Inserted ${rowMapArray.length} rows to table "${tableName}". New document.`);
    return;
  }

  const query = `
    SELECT tubeID
    FROM ${datasetName}.${tableName}
    WHERE d_132929440 = @d_132929440 AND d_789843387 = @d_789843387;
  `;
  const [rows] = await bigquery.query({ query, params: { d_132929440: boxId, d_789843387: siteCid } });
  const existingTubeIdSet = new Set(rows.map((row) => row.tubeID));
  if (existingTubeIdSet.size === 0) {
    await insertBoxRows(datasetName, tableName, docId, rowMapArray);
    console.log(`Inserted ${rowMapArray.length} rows to table "${tableName}".`);
    return;
  }

  const newTubeIdSet = tubeIdSet.difference(existingTubeIdSet);
  const overlappedTubeIdSet = tubeIdSet.intersection(existingTubeIdSet);
  const oudatedTubeIdSet = existingTubeIdSet.difference(tubeIdSet);

  if (newTubeIdSet.size > 0) {
    const newRows = rowMapArray.filter((row) => newTubeIdSet.has(row.get("tubeID")));
    await insertBoxRows(datasetName, tableName, docId, newRows);
    console.log(`Inserted ${newRows.length} rows to table "${tableName}". Part of box update.`);
  }

  if (overlappedTubeIdSet.size > 0) {
    const currRowMapArray = rowMapArray.filter((row) => overlappedTubeIdSet.has(row.get("tubeID")));
    for (const rowMap of currRowMapArray) {
      const query = `
        UPDATE ${datasetName}.${tableName}
        SET ${Array.from(rowMap.keys())
          .map((key) => `${key} = @${key}`)
          .join(", ")}
        WHERE d_132929440 = @d_132929440 AND d_789843387 = @d_789843387 AND tubeID = @tubeID AND updatedAt IN (
          SELECT MAX(updatedAt) 
          FROM ${datasetName}.${tableName} 
          WHERE d_132929440 = @d_132929440 AND d_789843387 = @d_789843387 AND tubeID = @tubeID );
          `;

      try {
        await bigquery.query({ query, params: Object.fromEntries(rowMap) });
      } catch (error) {
        await saveError(datasetName, tableName, docId, "UPDATE", rowMap, error);
      }

      // Todo: Check to see whether it's possible that current tube also exists in another box. If so, delete it from the other box.
    }
  }

  if (oudatedTubeIdSet.size > 0) {
    const query = `
      DELETE FROM ${datasetName}.${tableName}
      WHERE d_132929440 = @d_132929440 AND d_789843387 = @d_789843387 AND tubeID IN UNNEST(@tubeIdArray);
    `;
    try {
      await bigquery.query({
        query,
        params: { d_132929440: boxId, d_789843387: siteCid, tubeIdArray: Array.from(oudatedTubeIdSet) },
      });
      console.log(`Deleted ${oudatedTubeIdSet.size} rows from table "${tableName}". Part of box update.`);
    } catch (error) {
      await saveError(datasetName, tableName, docId, "DELETE", null, error);
    }
  }
};
