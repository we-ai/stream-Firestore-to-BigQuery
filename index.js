import functions from "@google-cloud/functions-framework";
import protobuf from "protobufjs";
import { streamDelete, streamUpdatesToBuffer, streamBoxesUpdatesToBuffer, syncBatchedUpdatesToTables } from "./utils.js";
import { tableNameArray } from "./settings.js";

const collectionNameSet = new Set(tableNameArray);
let DocumentEventData = null;
let isProtosLoaded = false;

const loadProtos = async () => {
  const root = await protobuf.load("data.proto");
  DocumentEventData = root.lookupType("google.events.cloud.firestore.v1.DocumentEventData");
  isProtosLoaded = true;
};

functions.cloudEvent("streamFirestoreUpdates", async (cloudEvent) => {
  if (!isProtosLoaded) {
    await loadProtos();
  }

  const decodedData = DocumentEventData.toObject(DocumentEventData.decode(cloudEvent.data), { longs: Number });
  const name = decodedData.value?.name || decodedData.oldValue.name;
  const [tableName, docId] = name.split("/").slice(-2);

  if (!collectionNameSet.has(tableName)) {
    console.log(`Doc changes in "${tableName}" collection are not streamed to BigQuery.`);
    return;
  }

  if (!decodedData.value) {
    console.log(`Deleting row (docId: ${docId}) from ${tableName} table in BigQuery...`);
    await streamDelete(tableName, docId);
    return;
  }

  const timestamp = new Date().toISOString();
  console.time(`Time for streaming ${tableName} doc ${docId}(${timestamp})`);
  if (tableName === "boxes") {
    await streamBoxesUpdatesToBuffer(tableName, docId, decodedData);
  } else {
    await streamUpdatesToBuffer(tableName, docId, decodedData);
  }

  console.timeEnd(`Time for streaming ${tableName} doc ${docId}(${timestamp})`);
});

functions.http("syncBatchedUpdatesToTables", async (req, res) => {
  console.time(`Time for syncing updates to all tables`);
  await syncBatchedUpdatesToTables();
  console.timeEnd(`Time for syncing updates to all tables`);
  return res.status(200).end();
});
