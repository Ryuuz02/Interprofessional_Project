#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dberror.h"
#include "tables.h"
#include "record_mgr.h"

// dynamic string
typedef struct VarString {
  char *buf;
  int size;
  int bufsize;
} VarString;

#define MAKE_VARSTRING(var)				\
  do {							\
  var = (VarString *) malloc(sizeof(VarString));	\
  var->size = 0;					\
  var->bufsize = 100;					\
  var->buf = malloc(100);				\
  } while (0)

#define FREE_VARSTRING(var)			\
  do {						\
  free(var->buf);				\
  free(var);					\
  } while (0)

#define GET_STRING(result, var)			\
  do {						\
    result = malloc((var->size) + 1);		\
    memcpy(result, var->buf, var->size);	\
    result[var->size] = '\0';			\
  } while (0)

#define RETURN_STRING(var)			\
  do {						\
    char *resultStr;				\
    GET_STRING(resultStr, var);			\
    FREE_VARSTRING(var);			\
    return resultStr;				\
  } while (0)

#define ENSURE_SIZE(var,newsize)				\
  do {								\
    if (var->bufsize < newsize)					\
    {								\
      int newbufsize = var->bufsize;				\
      while((newbufsize *= 2) < newsize);			\
      var->buf = realloc(var->buf, newbufsize);			\
    }								\
  } while (0)

#define APPEND_STRING(var,string)					\
  do {									\
    ENSURE_SIZE(var, var->size + strlen(string));			\
    memcpy(var->buf + var->size, string, strlen(string));		\
    var->size += strlen(string);					\
  } while(0)

#define APPEND(var, ...)			\
  do {						\
    char *tmp = malloc(10000);			\
    sprintf(tmp, __VA_ARGS__);			\
    APPEND_STRING(var,tmp);			\
    free(tmp);					\
  } while(0)

// Define NULL bitmap size macro
#define NULLBITMAP_SIZE(numAttr) (((numAttr) + 7) / 8)  // 1 bit per attribute, rounded up to nearest byte

// Prototypes
char *serializeNullBitmap(char *bitmap, int numAttr);

char *
serializeTableInfo(RM_TableData *rel)
{
  VarString *result;
  MAKE_VARSTRING(result);
  
  APPEND(result, "TABLE <%s> with <%i> tuples:\n", rel->name, getNumTuples(rel));
  APPEND_STRING(result, serializeSchema(rel->schema));
  
  RETURN_STRING(result);  
}

char * 
serializeTableContent(RM_TableData *rel)
{
  int i;
  VarString *result;
  RM_ScanHandle *sc = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
  Record *r = (Record *) malloc(sizeof(Record));
  MAKE_VARSTRING(result);

  for(i = 0; i < rel->schema->numAttr; i++)
    APPEND(result, "%s%s", (i != 0) ? ", " : "", rel->schema->attrNames[i]);

  startScan(rel, sc, NULL);
  
  while(next(sc, r) != RC_RM_NO_MORE_TUPLES) 
    {
    APPEND_STRING(result, serializeRecord(r, rel->schema));
    APPEND_STRING(result, "\n");
    }
  closeScan(sc);

  RETURN_STRING(result);
}


char * 
serializeSchema(Schema *schema)
{
  int i;
  VarString *result;
  MAKE_VARSTRING(result);

  APPEND(result, "Schema with <%i> attributes (", schema->numAttr);

  for(i = 0; i < schema->numAttr; i++)
    {
      APPEND(result, "%s%s: ", (i != 0) ? ", ": "", schema->attrNames[i]);
      switch (schema->dataTypes[i])
	{
	case DT_INT:
	  APPEND_STRING(result, "INT");
	  break;
	case DT_FLOAT:
	  APPEND_STRING(result, "FLOAT");
	  break;
	case DT_STRING:
	  APPEND(result, "STRING[%i]", schema->typeLength[i]);
	  break;
	case DT_BOOL:
	  APPEND_STRING(result, "BOOL");
	  break;
	case DT_NULL:
	  APPEND_STRING(result, "NULL");  // Handle DT_NULL
	  break;
	}
    }
  APPEND_STRING(result, ")");

  APPEND_STRING(result, " with keys: (");

  for(i = 0; i < schema->keySize; i++)
    APPEND(result, "%s%s", ((i != 0) ? ", ": ""), schema->attrNames[schema->keyAttrs[i]]); 

  APPEND_STRING(result, ")\n");

  RETURN_STRING(result);
}

char * 
serializeRecord(Record *record, Schema *schema)
{
  VarString *result;
  MAKE_VARSTRING(result);
  int i;

  // Add NULL bitmap
  int numAttr = schema->numAttr;
  char *nullBitmap = (char *)malloc(NULLBITMAP_SIZE(numAttr));  // Create a bitmap for NULL values
  memset(nullBitmap, 0, NULLBITMAP_SIZE(numAttr));  // Initialize the bitmap to all 0 (not NULL)

  // Check for NULL values and set bits in the bitmap
  for (i = 0; i < numAttr; i++) {
    Value *value;
    getAttr(record, schema, i, &value);  // Fetch value of attribute
    if (value->dt == DT_NULL) {
      nullBitmap[i / 8] |= (1 << (i % 8));  // Set the bit for this attribute as NULL
    }
    freeVal(value);  // Free the value after checking
  }

  // Append NULL bitmap to the record serialization
  APPEND(result, "[NULLBITMAP:%s] ", serializeNullBitmap(nullBitmap, numAttr));

  // Serialize the rest of the attributes
  APPEND(result, "[%i-%i] (", record->id.page, record->id.slot);

  for (i = 0; i < schema->numAttr; i++)
  {
    Value *value;
    getAttr(record, schema, i, &value);
    if (value->dt != DT_NULL) {
      APPEND_STRING(result, serializeAttr(record, schema, i));  // Only serialize non-NULL attributes
    } else {
      APPEND(result, "%s:NULL", schema->attrNames[i]);  // Represent NULL values
    }
    APPEND(result, "%s", (i == 0) ? "" : ",");
    freeVal(value);  // Free the value after serializing
  }

  APPEND_STRING(result, ")");

  free(nullBitmap);  // Free the NULL bitmap memory
  RETURN_STRING(result);
}

char * 
serializeAttr(Record *record, Schema *schema, int attrNum)
{
  int offset;
  char *attrData;
  VarString *result;
  MAKE_VARSTRING(result);

  // Check if the attribute is NULL using the bitmap
  Value *value;
  getAttr(record, schema, attrNum, &value);

  if (value->dt == DT_NULL) {
    APPEND(result, "%s:NULL", schema->attrNames[attrNum]);  // Handle DT_NULL
    freeVal(value);
    RETURN_STRING(result);
  }

  // Non-NULL case: get the offset and serialize the attribute data
  attrOffset(schema, attrNum, &offset);
  attrData = record->data + offset;

  switch (schema->dataTypes[attrNum])
  {
    case DT_INT:
      {
        int val = 0;
        memcpy(&val, attrData, sizeof(int));
        APPEND(result, "%s:%i", schema->attrNames[attrNum], val);
      }
      break;
    case DT_STRING:
      {
        char *buf;
        int len = schema->typeLength[attrNum];
        buf = (char *) malloc(len + 1);
        strncpy(buf, attrData, len);
        buf[len] = '\0';
        
        APPEND(result, "%s:%s", schema->attrNames[attrNum], buf);
        free(buf);
      }
      break;
    case DT_FLOAT:
      {
        float val;
        memcpy(&val, attrData, sizeof(float));
        APPEND(result, "%s:%f", schema->attrNames[attrNum], val);
      }
      break;
    case DT_BOOL:
      {
        bool val;
        memcpy(&val, attrData, sizeof(bool));
        APPEND(result, "%s:%s", schema->attrNames[attrNum], val ? "TRUE" : "FALSE");
      }
      break;
    default:
      return "NO SERIALIZER FOR DATATYPE";
  }

  freeVal(value);  // Free the value after use
  RETURN_STRING(result);
}

char *
serializeValue(Value *val)
{
  VarString *result;
  MAKE_VARSTRING(result);
  
  switch(val->dt)
  {
    case DT_INT:
      APPEND(result, "%i", val->v.intV);
      break;
    case DT_FLOAT:
      APPEND(result, "%f", val->v.floatV);
      break;
    case DT_STRING:
      APPEND(result, "%s", val->v.stringV);
      break;
    case DT_BOOL:
      APPEND_STRING(result, ((val->v.boolV) ? "true" : "false"));
      break;
    case DT_NULL:
      APPEND_STRING(result, "NULL");  // Handle DT_NULL
      break;
  }

  RETURN_STRING(result);
}

Value *
stringToValue(char *val)
{
  Value *result = (Value *) malloc(sizeof(Value));
  
  switch(val[0])
  {
    case 'i':
      result->dt = DT_INT;
      result->v.intV = atoi(val + 1);
      break;
    case 'f':
      result->dt = DT_FLOAT;
      result->v.floatV = atof(val + 1);
      break;
    case 's':
      result->dt = DT_STRING;
      result->v.stringV = malloc(strlen(val));
      strcpy(result->v.stringV, val + 1);
      break;
    case 'b':
      result->dt = DT_BOOL;
      result->v.boolV = (val[1] == 't') ? TRUE : FALSE;
      break;
    case 'n':  // Assuming 'n' is used for NULL values
      result->dt = DT_NULL;
      break;
    default:
      result->dt = DT_INT;
      result->v.intV = -1;
      break;
  }
  
  return result;
}


RC
attrOffset(Schema *schema, int attrNum, int *result)
{
  int offset = 0;
  int attrPos = 0;

  for (attrPos = 0; attrPos < attrNum; attrPos++) {
    switch (schema->dataTypes[attrPos]) {
      case DT_STRING:
        offset += schema->typeLength[attrPos] + 1;
        break;
      case DT_INT:
        offset += sizeof(int);
        break;
      case DT_FLOAT:
        offset += sizeof(float);
        break;
      case DT_BOOL:
        offset += sizeof(bool);
        break;
      case DT_NULL:
        // No space required for NULL values
        break;
    }
  }

  *result = offset;
  return RC_OK;
}

char *serializeNullBitmap(char *bitmap, int numAttr)
{
  VarString *result;
  MAKE_VARSTRING(result);

  for (int i = 0; i < NULLBITMAP_SIZE(numAttr); i++) {
    APPEND(result, "%02X", (unsigned char)bitmap[i]);  // Convert each byte of the bitmap to hex
  }

  RETURN_STRING(result);
}
