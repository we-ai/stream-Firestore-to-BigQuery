export const bufferDatasetName = "firestore_stream_buffer"; // Name of the dataset holding the buffer tables. Each buffer table has an additional field `isDeleted` (type BOOLEAN) compared to corresponding target table.
export const bufferRecordsRetainHours = 24; // Number of hours to retain streamed records in buffer tables.
export const datasetName = "firestore_stream"; // Name of dataset holding the target tables.
export const errorLogTableName = "error_log"; // Name of table storing errors.
export const warningLogTableName = "warning_log"; // Name of table storing warnings.
export const tableNameArray = [
  "bioSurvey_v1",
  "biospecimen",
  "birthdayCard",
  "boxes",
  "cancerOccurrence",
  "cancerScreeningHistorySurvey",
  "clinicalBioSurvey_v1",
  "covid19Survey_v1",
  "experience2024",
  "kitAssembly",
  "menstrualSurvey_v1",
  "module1_v1",
  "module1_v2",
  "module2_v1",
  "module2_v2",
  "module3_v1",
  "module4_v1",
  "mouthwash_v1",
  "notifications",
  "participants",
  "promis_v1",
]; // Table names in BigQuery (collection names in Firestore)

export const collectionNameArray = tableNameArray;
