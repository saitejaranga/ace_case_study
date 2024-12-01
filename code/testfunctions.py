from pyspark.sql import types as T

# Function to compare two schemas
def compare_schemas(schema1, schema2):
    fields1 = {field.name: field.dataType for field in schema1.fields}
    fields2 = {field.name: field.dataType for field in schema2.fields}

    # Find fields in schema1 not in schema2
    diff1 = {name: dtype for name, dtype in fields1.items() if name not in fields2}
    
    # Find fields in schema2 not in schema1
    diff2 = {name: dtype for name, dtype in fields2.items() if name not in fields1}
    
    # Find fields that are in both schemas but with different types
    common_fields = set(fields1.keys()).intersection(set(fields2.keys()))
    type_mismatches = {name: (fields1[name], fields2[name]) for name in common_fields if fields1[name] != fields2[name]}
    
    return diff1, diff2, type_mismatches

