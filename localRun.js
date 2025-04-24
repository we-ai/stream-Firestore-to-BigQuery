#!/usr/bin/env node
import { execSync } from "node:child_process";
import {
  createAllBufferTables,
  createAllTargetTables,
  createLogTables,
  parseArgs,
  getAndSaveSchemas,
  cleanupSchemasFile,
  simplifySchemaFields,
  formatSchemasFile,
  updateSchemasInBigQuery,
  flattenCidArray,
  getAndSaveAllRowsFromWarningLogs,
  mergeWarningFieldsToSchemas,
} from "./localUtils.js";

const projectTiers = {
  dev: "nih-nci-dceg-connect-dev",
  stage: "nih-nci-dceg-connect-stg-5519",
  prod: "nih-nci-dceg-connect-prod-6d04",
};

const commandLineEntries = {
  createAllBufferTables,
  createAllTargetTables,
  createLogTables,
  getAndSaveSchemas,
  cleanupSchemasFile,
  simplifySchemaFields,
  flattenCidArray,
  formatSchemasFile,
  getAndSaveAllRowsFromWarningLogs,
  mergeWarningFieldsToSchemas,
  updateSchemasInBigQuery,
};

/**
 *
 * @param {string[]} inputArgs
 * @returns void
 * @description Parses command line arguments and executes the corresponding function.
 * @example
 * node localRun.js --entry createAllBufferTables --gcloud --env dev
 * node localRun.js --entry updateSchemasInBigQuery participants boxes --gcloud --env stage
 */
const runCommand = async (inputArgs) => {
  const argObj = parseArgs(inputArgs);
  const gcloud = argObj.gcloud || false;
  const tierString = argObj.env || "dev";

  if (gcloud) {
    if (!projectTiers[tierString]) {
      console.log(`Invalid environment: ${tierString}`);
      return;
    }

    execSync(`gcloud config set project ${projectTiers[tierString]}`);
    const projectInfo = execSync("gcloud config list", { encoding: "utf-8" });
    console.log(projectInfo);
  }

  const entryFunction = commandLineEntries[argObj.entry];
  if (!entryFunction) {
    console.log(`'Invalid entry function: ${argObj.entry}`);
    return;
  }

  entryFunction(...argObj.args);
};

const runFile = async (gcloud, tierString) => {
  if (gcloud) {
    if (!projectTiers[tierString]) {
      console.log(`Invalid tier "${tierString}" for project.`);
      return;
    }

    execSync(`gcloud config set project ${projectTiers[tierString]}`); // no need to set project for local file processing
    const projectInfo = execSync("gcloud config list", { encoding: "utf-8" });
    console.log(projectInfo);
  }

  /**
   * Create tables in BigQuery
   */
  // await createAllBufferTables();    // Create tables in BigQuery, using defined schemas
  // await createLogTables();            // Create "error_log" and "warning_log" tables

  /**
   * Get current schemas and compare with base schemas
   */
  // await getAndSaveSchemas(tierString);
  // await cleanupSchemasFile(`./temp/schemas/${tierString}_Connect_schemas.json`);
  // await simplifySchemaFields(`./temp/schemas/${tierString}_Connect_schemas.json`);
  // await flattenCidArray("./temp/schemas_with_flattened_cid.json"); // Create "schemas_with_flattened_arrays.json" file
  // await compareSchemas("./temp/schemas.json", "./temp/dev_Connect_schemas_cleaned.json", "./temp/base_vs_dev_result.json");

  /**
   * Format schemas and split to individual files for easier reviews
   */
  // await formatSchemasFile("./schemas.json", "./schemas_reordered.json");
  // await splitSchemas("./schemas.json", "./schemas/schemaForEachTable");

  /**
   * Update schemas based on warning records. After reviewing updated schemas, apply the schemas to BigQuery tables.
   */
  // await getAndSaveAllRowsFromWarningLogs(); // Need to check the output file in `./temp/schemas` folder
  // await mergeWarningFieldsToSchemas();
  // await updateSchemasInBigQuery("participants", "biospecimen");

  // const fieldArray = [
  //   {oldName: "createTime", newName: "createdAt"},
  //   {oldName: "updateTime", newName: "updatedAt"},
  // ];

  // for (const collectionName of collectionNameArray) {
  //   if (collectionName === "biospecimen") {
  //     continue;
  //   }
  //   await changeFieldNames("firestore_stream", collectionName, fieldArray);
  // }

  /**
   * Sync updates to target tables in BigQuery
   */
  // await syncUpdates("participants");
};

const main = async (gcloud = false, tierString = "dev") => {
  console.time("Time taken to run");
  const args = process.argv.slice(2);
  if (args.length > 0) {
    await runCommand(args);
  } else {
    await runFile(gcloud, tierString);
  }

  console.timeEnd("Time taken to run");
};

main();
