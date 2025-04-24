import { BigQuery } from "@google-cloud/bigquery";
import { bufferDatasetName, bufferRecordsRetainHours, datasetName, errorLogTableName, warningLogTableName, tableNameArray } from "./settings.js";
import allSchemas from "./tableSchemas.js";
import arraysToBeFlattened from "./arraysToBeFlattened.js";

export const bigquery = new BigQuery();
const mixedStringAndObjectTypes = {
  covid19Survey_v1: new Set(["D_114280729", "D_749956170"]),
  module2_v2: new Set(["D_128705365"]),
  module3_v1: new Set(["D_470862706", "D_633553324"]),
  module4_v1: new Set(["D_135529881", "D_219317801", "D_440093675", "D_679430807", "D_786253125", "D_968388901"]),
};

export let allTableFieldNameSets = {};
for (const tableName in allSchemas) {
  allTableFieldNameSets[tableName] = new Set(allSchemas[tableName].map((field) => field.name));
}

const wrongKeys = new Set(["undefined"]);
export const ignoredKeys = {
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
    ]),
    nested: new Set(["treeJSON", "COMPLETED", "COMPLETED_TS", "sha", "110349197", "543608829"]),
  },
  bioSurvey_v1: {
    top: new Set(["treeJSON", "sha"]),
  },
  cancerScreeningHistorySurvey: {
    top: new Set(["treeJSON", "sha"]),
  },
  clinicalBioSurvey_v1: {
    top: new Set(["treeJSON", "sha"]),
  },
  covid19Survey_v1: {
    top: new Set(["treeJSON", "sha"]),
  },
  experience2024: {
    top: new Set(["treeJSON", "sha"]),
  },
  menstrualSurvey_v1: {
    top: new Set(["treeJSON", "sha"]),
  },
  module1_v1: {
    top: new Set(["treeJSON", "sha"]),
  },
  module1_v2: {
    top: new Set(["treeJSON", "sha"]),
  },
  module2_v1: {
    top: new Set(["treeJSON", "sha"]),
  },
  module2_v2: {
    top: new Set(["treeJSON", "sha"]),
  },
  module3_v1: {
    top: new Set(["treeJSON", "sha"]),
  },
  module4_v1: {
    top: new Set(["treeJSON", "sha"]),
  },
  mouthwash_v1: {
    top: new Set(["treeJSON", "sha"]),
  },
  promis_v1: {
    top: new Set(["treeJSON", "sha"]),
  },
};

/**
 * 
 * @param {string} datasetName
 * @param {string} targetTable 
 * @param {string} docId 
 * @param {string} operation 
 * @param {object | null} data 
 * @param {Error} error 
 */
export const saveError = async (datasetName, targetTable, docId, operation, data, error) => {
  let stringifiedData = data;
  if (data && typeof data === "object") {
    stringifiedData = JSON.stringify(data);
  } else {
    if (data !== null) {
      console.error(`Unexpected data type sent to error log: ${typeof data}`);
      return;
    }
  }

  const row = {
    targetTable,
    docId,
    operation,
    data: stringifiedData,
    errorTime: new Date().toISOString(),
    errorDetails: error.errors ? JSON.stringify(error.errors) : error.message,
  };

  try {
    await bigquery.dataset(datasetName).table(errorLogTableName).insert(row);
  } catch (err) {
    console.error(`Error occurred when saving to log table "${errorLogTableName}".`, JSON.stringify(err, null, 2));
  }
};

export const saveWarning = async (datasetName, tableName, docId, data, msg = "") => {
  const row = {
    targetTable: tableName,
    docId,
    data: data ? JSON.stringify(data) : null,
    warningTime: new Date().toISOString(),
    warningDetails: msg,
  };

  try {
    await bigquery.dataset(datasetName).table(warningLogTableName).insert(row);
  } catch (err) {
    console.error(`Error occurred when saving to log table "${warningLogTableName}".`, JSON.stringify(err, null, 2));
  }
};

export const cleanupFieldsData = (tableName, fieldsData) => {
  let resultData = {};
  let stack = [];
  let warningMsgArray = [];

  if (typeof fieldsData === "object" && fieldsData !== null) {
    for (let key in fieldsData) {
      if (ignoredKeys[tableName]?.top.has(key)) continue;
      if (wrongKeys.has(key)) {
        warningMsgArray.push(`Key "${key}" found in event data `);
        continue;
      }
      stack.push({ parent: resultData, key: key.replace(/^(\d)/, "d_$1"), value: fieldsData[key] });
    }
  } else {
    return [fieldsData, warningMsgArray];
  }

  while (stack.length > 0) {
    const { parent, key, value } = stack.pop();

    if (Array.isArray(value)) {
      parent[key] = [];
      value.forEach((item, index) => {
        stack.push({ parent: parent[key], key: index, value: item });
      });
    } else if (value && typeof value === "object") {
      for (const k in value) {
        if (k === "nullValue" || ignoredKeys[tableName]?.nested?.has(k)) continue;
        if (k === "stringValue" || k === "booleanValue") {
          parent[key] = value[k];
        } else if (k === "integerValue") {
          parent[key] = value[k].toString();
        } else if (k === "mapValue") {
          parent[key] = {};
          for (const mapKey in value[k].fields) {
            stack.push({
              parent: parent[key],
              key: mapKey.replace(/^(\d)/, "d_$1"),
              value: value[k].fields[mapKey],
            });
          }
        } else if (k === "arrayValue") {
          const valueArray = value[k].values;
          if (Array.isArray(valueArray)) {
            parent[key] = [];
            valueArray.forEach((item, index) => {
              stack.push({ parent: parent[key], key: index, value: item });
            });
          } else if (
            typeof value.arrayValue === "object" &&
            value.arrayValue !== null &&
            Object.keys(value.arrayValue).length === 0
          ) {
            continue; // Ignore empty array. otherwhise `parent[key] = [];`
          } else {
            warningMsgArray.push(`Unusual values found in array. key: ${key}; value: ${JSON.stringify(value, null, 2)}`);
          }
        } else {
          parent[key] = {};
          stack.push({ parent: parent[key], key: k.replace(/^(\d)/, "d_$1"), value: value[k] });
        }
      }
    } else {
      parent[key] = value;
    }
  }

  return [resultData, warningMsgArray];
};

const recoverArray = (valueArray, warningMsgArray) => {
  const result = [];
  for (const item of valueArray) {
    const [key, value] = Object.entries(item)[0];
    if (key === "stringValue" || key === "booleanValue") {
      result.push(value);
    } else if (key === "integerValue") {
      result.push(value.toString());
    } else {
      warningMsgArray.push(`Unexpected key "${key}" found in array.`);
    }
  }

  return result;
};

const flattenArrayToObject = (tableName, path, valueArray) => {
  let result = {};
  const refValueArray = arraysToBeFlattened[tableName]?.[path];
  if (!refValueArray) return result;
  const inputValues = new Set(valueArray);
  for (const refValue of refValueArray) {
    const currPath = `${path}_${refValue.replace(/^(\d)/, "D_$1")}`;
    if (inputValues.has(refValue)) {
      result[currPath] = "1";
    } else {
      result[currPath] = "0";
    }
  }

  return result;
};

export const cleanupAndFlattenFields = (tableName, fieldsData) => {
  let flattenedData = {};
  let stack = [];
  let warningMsgArray = [];

  if (fieldsData && typeof fieldsData === "object") {
    for (let key in fieldsData) {
      if (ignoredKeys[tableName]?.top.has(key)) continue;
      if (wrongKeys.has(key)) {
        warningMsgArray.push(`Key "${key}" found in event data `);
        continue;
      }
      stack.push({ key: key.replace(/\./g, "_").replace(/^(\d)/, "d_$1"), value: fieldsData[key] });
    }
  } else {
    return [fieldsData, warningMsgArray];
  }

  while (stack.length > 0) {
    const { key, value } = stack.pop();
    if (value && typeof value === "object") {
      for (const k in value) {
        if (k === "nullValue" || ignoredKeys[tableName]?.nested?.has(k)) continue;
        if (k === "stringValue" || k === "booleanValue") {
          if (mixedStringAndObjectTypes[tableName]?.has(key)) {
            const adjustedKey = `${key}_string`;
            flattenedData[adjustedKey] = value[k];
          } else {
            flattenedData[key] = value[k];
          }
        } else if (k === "integerValue") {
          flattenedData[key] = value[k].toString();
        } else if (k === "mapValue") {
          for (const mapKey in value[k].fields) {
            stack.push({
              key: `${key}_${mapKey.replace(/^(\d)/, "d_$1")}`,
              value: value[k].fields[mapKey],
            });
          }
        } else if (k === "arrayValue") {
          const recoveredArray = recoverArray(value[k].values, warningMsgArray);
          if (arraysToBeFlattened[tableName]?.[key]) {
            const obj = flattenArrayToObject(tableName, key, recoveredArray);
            Object.assign(flattenedData, obj);
          } else {
            flattenedData[key] = recoveredArray;
          }
        } else {
          stack.push({ key: `${key}_${k.replace(/^(\d)/, "d_$1")}`, value: value[k] });
        }
      }
    } else {
      flattenedData[key] = value;
    }
  }

  return [flattenedData, warningMsgArray];
};

export const streamDelete = async (tableName, docId) => {
  const row = { docId, updatedAt: new Date().toISOString(), isDeleted: true };

  try {
    await bigquery.dataset(bufferDatasetName).table(tableName).insert(row);
  } catch (error) {
    await saveError(datasetName, tableName, docId, "DELETE", null, error);
  }
};

export const streamInsert = async (tableName, rowObj) => {
  try {
    await bigquery.dataset(bufferDatasetName).table(tableName).insert(rowObj);
  } catch (error) {
    await saveError(datasetName, tableName, rowObj.docId, "INSERT", rowObj, error);
  }
};

export const streamUpdatesToBuffer = async (tableName, docId, decodedData) => {
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
      console.log(`All fields in updateMask are ignored for table "${tableName}".`);
      return;
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
    await saveWarning(datasetName, tableName, docId, flattenedData, warningMsgArray.sort().join("; "));

    extraFieldsInData.forEach((fieldName) => {
      delete flattenedData[fieldName];
    });
  }

  const createTimeMilliseconds =
    decodedData.value.createTime.seconds * 1000 + Math.round(decodedData.value.createTime.nanos / 1e6);
  const createdAt = new Date(createTimeMilliseconds).toISOString();
  const updateTimeMilliseconds =
    decodedData.value.updateTime.seconds * 1000 + Math.round(decodedData.value.updateTime.nanos / 1e6);
  const updatedAt = new Date(updateTimeMilliseconds).toISOString();
  const rowObj = { docId, createdAt, updatedAt, ...flattenedData };
  await streamInsert(tableName, rowObj);
};

const bagCidArray = [
  "d_650224161",
  "d_136341211",
  "d_503046679",
  "d_313341808",
  "d_668816010",
  "d_754614551",
  "d_174264982",
  "d_550020510",
  "d_673090642",
  "d_492881559",
  "d_536728814",
  "d_309413330",
  "d_357218702",
  "d_945294744",
  "d_741697447",
];

const boxRowCommonKeyArray = [
  "d_672863981",
  "d_560975149",
  "d_842312685",
  "d_132929440",
  "d_789843387",
  "d_555611076",
  "d_145971562",
  "d_959708259",
  "d_948887825",
  "d_666553960",
  "d_885486943",
  "d_656548982",
  "d_105891443",
  "d_238268405",
  "d_870456401",
  "d_926457119",
  "d_333524031",
];

/**
 * Convert a doc of json structure to multiple rows in  BigQuery table.
 * Each box doc in Firestore can be converted to multiple rows in BigQuery table.
 * @param {object} boxData
 */
export const flattenBoxData = (boxData) => {
  let rowObjArray = [];
  let tubeIdSet = new Set();
  let rowCommonData = {};

  boxRowCommonKeyArray.forEach((key) => {
    if (Array.isArray(boxData[key])) {
      rowCommonData[key] = boxData[key].join(";");
    } else {
      rowCommonData[key] = boxData[key];
    }
  });


  for (const bagCid of bagCidArray) {
    const hasTube = boxData[bagCid]?.d_234868461?.length > 0;
    if (!hasTube) continue;

    let bagType = "";
    if (boxData[bagCid].d_787237543) {
      bagType = "Blood/Urine";
    } else if (boxData[bagCid].d_223999569) {
      bagType = "Mouth wash";
    } else if (boxData[bagCid].d_522094118) {
      bagType = "Orphan bag";
    }

    const currTubeIdArray = boxData[bagCid].d_234868461;
    for (const tubeID of currTubeIdArray) {
      rowObjArray.push({
        ...rowCommonData,
        bagID: boxData[bagCid].d_787237543 || boxData[bagCid].d_223999569 || boxData[bagCid].d_522094118,
        bagType,
        d_469819603: boxData[bagCid].d_469819603 || "",
        d_255283733: boxData[bagCid].d_255283733,
        tubeID,
      });
      tubeIdSet.add(tubeID);
    }
  }

  return rowObjArray;
};

export const streamBoxesUpdatesToBuffer = async (tableName, docId, decodedData) => {
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
      console.log(`All fields in updateMask are ignored for table "${tableName}".`);
      return;
    }
  }

  const [boxData, warningMsgArray] = cleanupFieldsData(tableName, decodedData.value.fields);
  if (warningMsgArray.length > 0) {
    await saveWarning(datasetName, tableName, docId, boxData, warningMsgArray.sort().join("; "));
  }

  let rowObjArray = flattenBoxData(boxData);
  if (rowObjArray.length === 0) {
    console.log(`No rows to insert to table "${tableName}".`);
    return;
  }

  const createTimeMilliseconds =
    decodedData.value.createTime.seconds * 1000 + Math.round(decodedData.value.createTime.nanos / 1e6);
  const createdAt = new Date(createTimeMilliseconds).toISOString();
  const updateTimeMilliseconds =
    decodedData.value.updateTime.seconds * 1000 + Math.round(decodedData.value.updateTime.nanos / 1e6);
  const updatedAt = new Date(updateTimeMilliseconds).toISOString();
  rowObjArray = rowObjArray.map((rowObj) => ({ docId, createdAt, updatedAt, ...rowObj }));

  let promiseArray = [];
  for (const rowObj of rowObjArray) {
    promiseArray.push(streamInsert(tableName, rowObj));
  }

  await Promise.allSettled(promiseArray);
};

/**
 * Delete old records from buffer tables after a certain period of time.
 * @param {string} tableName 
 */
const deleteOldRecordsFromBuffer = async (tableName) => {
  let cutoffTime = new Date();
  cutoffTime.setHours(cutoffTime.getHours() - bufferRecordsRetainHours);
  const cutoffTimeStr = cutoffTime.toISOString();
  const deleteOldRecords = `
    DELETE FROM ${bufferDatasetName}.${tableName}
    WHERE updatedAt < "${cutoffTimeStr}";
    `;

  try {
    await bigquery.query(deleteOldRecords);
  } catch (error) {
    saveError(datasetName, tableName, null, "DELETE", null, error);
  }
};

export const syncUpdates = async (tableName) => {
  const fieldNameArray = Array.from(allTableFieldNameSets[tableName]);
  const syncBatchedUpdates = `
    MERGE ${datasetName}.${tableName} T
    USING (
      SELECT * 
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY docId ORDER BY updatedAt DESC) AS rn
        FROM ${bufferDatasetName}.${tableName} ) 
      WHERE rn = 1 ) S
    ON T.docId = S.docId
    WHEN MATCHED AND S.isDeleted IS TRUE THEN DELETE
    WHEN MATCHED AND S.updatedAt > T.updatedAt THEN 
      UPDATE SET ${fieldNameArray.map((fieldName) => `T.${fieldName} = S.${fieldName}`).join(", ")}
    WHEN NOT MATCHED AND S.isDeleted IS NOT TRUE THEN 
      INSERT (${fieldNameArray.join(", ")}) VALUES (${fieldNameArray.map((fieldName) => `S.${fieldName}`).join(", ")});
      `;

  try {
    await bigquery.query(syncBatchedUpdates);
    await deleteOldRecordsFromBuffer(tableName);
    console.log(`Synced updates to ${datasetName}.${tableName}.`);
  } catch (error) {
    saveError(datasetName, tableName, null, "MERGE", null, error);
  }
}

/**
 * "boxes" table is different. One doc in "boxes" collection can be converted into multiple rows for '"boxes" table.
 * @param {string} tableName 
 */
export const syncBoxesUpdates = async (tableName) => {
  const fieldNameArray = Array.from(allTableFieldNameSets[tableName]);

    /**
   * This query handles insertion of new tubes and updating existing tubes.
   * But it doesn't handle tube deletion cases: 
   *  1) tubes are moved out of box;
   *  2) tubes should deleted because of deletion of an entire 'boxes' document holding the tubes.
   * Separate queries are needed for these cases.
   */
  const syncBatchedBoxesUpdates = `
    MERGE ${datasetName}.${tableName} T
    USING (
      SELECT * 
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY tubeID ORDER BY updatedAt DESC) AS rn
        FROM ${bufferDatasetName}.${tableName} ) 
      WHERE rn = 1 ) S
    ON T.tubeID = S.tubeID
    WHEN MATCHED AND S.updatedAt > T.updatedAt THEN 
      UPDATE SET ${fieldNameArray.map((fieldName) => `T.${fieldName} = S.${fieldName}`).join(", ")}
    WHEN NOT MATCHED AND S.isDeleted IS NOT TRUE THEN 
      INSERT (${fieldNameArray.join(", ")}) VALUES (${fieldNameArray.map((fieldName) => `S.${fieldName}`).join(", ")});
  `;

  const deleteTubesMovedOutOfBox = `
    DELETE FROM ${datasetName}.${tableName} 
    WHERE STRUCT(tubeID, d_132929440, d_789843387) IN (
      SELECT STRUCT(T.tubeID, T.d_132929440, T.d_789843387)
      FROM (
        SELECT *
        FROM ${datasetName}.${tableName}
        WHERE STRUCT(d_132929440, d_789843387) IN (
            SELECT DISTINCT STRUCT(d_132929440, d_789843387)
            FROM
            ${bufferDatasetName}.${tableName}) ) AS T
      JOIN
        ${bufferDatasetName}.${tableName} AS S
      USING (tubeID)
      WHERE
        S.tubeID IS NULL);
    `;

  const deleteTubesAfterBoxDocDeletion = `
    DELETE FROM ${datasetName}.${tableName}
    WHERE docId IN (
      SELECT DISTINCT docId
      FROM ${bufferDatasetName}.${tableName}
      WHERE isDeleted IS TRUE);
    `;

  try {
    await bigquery.query(syncBatchedBoxesUpdates);
    await bigquery.query(deleteTubesMovedOutOfBox);
    await bigquery.query(deleteTubesAfterBoxDocDeletion);
    await deleteOldRecordsFromBuffer(tableName);
    console.log(`Synced updates to table ${datasetName}.${tableName}.`);
  } catch (error) {
    saveError(datasetName, tableName, null, "MERGE", null, error);
  }
};

export const syncBatchedUpdatesToTables = async () => {
  let promiseArray = [];
  for (const tableName of tableNameArray) {
    if (tableName === "boxes") {
      promiseArray.push(syncBoxesUpdates(tableName));
    } else {
      promiseArray.push(syncUpdates(tableName));
    }
  }

  await Promise.allSettled(promiseArray);
};
