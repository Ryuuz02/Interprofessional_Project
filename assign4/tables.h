#ifndef TABLES_H
#define TABLES_H

#include "dt.h"

// Data Types, Records, and Schemas
typedef enum DataType {
  DT_INT = 0,
  DT_STRING = 1,
  DT_FLOAT = 2,
  DT_BOOL = 3,
  DT_NULL = 4   // Added DT_NULL for representing NULL values
} DataType;

typedef struct Value {
  DataType dt;
  union v {
    int intV;
    char *stringV;
    float floatV;
    bool boolV;
  } v;
} Value;

typedef struct RID {
  int page;
  int slot;
} RID;

typedef struct Record
{
  RID id;
  char *data;
  char *nullBitmap;  // Added: Bitmap for tracking which attributes are NULL
} Record;

// Information of a table schema: its attributes, data types, and primary keys
typedef struct Schema
{
  int numAttr;
  char **attrNames;
  DataType *dataTypes;
  int *typeLength;
  int *keyAttrs;
  int keySize;
  bool *nullable;  // Added: Boolean array indicating if each attribute can be NULL
} Schema;

// TableData: Management Structure for a Record Manager to handle one relation
typedef struct RM_TableData
{
  char *name;
  Schema *schema;
  void *mgmtData;
} RM_TableData;

// Scan Handle for managing record scans
// typedef struct RM_ScanHandle
// {
//   RM_TableData *rel;
//   void *mgmtData;
//   int *sortAttrs;  // List of attributes to sort on
// } RM_ScanHandle;

// Macros for handling NULL values
#define IS_NULL(record, attrNum) \
  (record->nullBitmap[attrNum / 8] & (1 << (attrNum % 8)))

#define SET_NULL(record, attrNum) \
  (record->nullBitmap[attrNum / 8] |= (1 << (attrNum % 8)))

#define UNSET_NULL(record, attrNum) \
  (record->nullBitmap[attrNum / 8] &= ~(1 << (attrNum % 8)))

// Macros for creating values, including NULL values
#define MAKE_STRING_VALUE(result, value)                \
  do {                                                  \
    (result) = (Value *) malloc(sizeof(Value));         \
    (result)->dt = DT_STRING;                           \
    (result)->v.stringV = (char *) malloc(strlen(value) + 1); \
    strcpy((result)->v.stringV, value);                 \
  } while(0)

#define MAKE_VALUE(result, datatype, value)             \
  do {                                                  \
    (result) = (Value *) malloc(sizeof(Value));         \
    (result)->dt = datatype;                            \
    switch(datatype)                                    \
      {                                                 \
      case DT_INT:                                      \
        (result)->v.intV = value;                       \
        break;                                          \
      case DT_FLOAT:                                    \
        (result)->v.floatV = value;                     \
        break;                                          \
      case DT_BOOL:                                     \
        (result)->v.boolV = value;                      \
        break;                                          \
      case DT_NULL:                                     \
        break;                                          \
      }                                                 \
  } while(0)

#define MAKE_NULL_VALUE(result)                         \
  do {                                                  \
    (result) = (Value *) malloc(sizeof(Value));         \
    (result)->dt = DT_NULL;                             \
  } while(0)

#define IS_NULL_VALUE(val)  ((val)->dt == DT_NULL)

// Debug and read methods
extern Value *stringToValue (char *value);
extern char *serializeTableInfo(RM_TableData *rel);
extern char *serializeTableContent(RM_TableData *rel);
extern char *serializeSchema(Schema *schema);
extern char *serializeRecord(Record *record, Schema *schema);
extern char *serializeAttr(Record *record, Schema *schema, int attrNum);
extern char *serializeValue(Value *val);

extern RC attrOffset (Schema *schema, int attrNum, int *result);

#endif
