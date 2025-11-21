from spark import main as spark_main
import pytest

def test_spark_job():
    df = spark_main()
    assert df is not None # Check that the DataFrame is not None
    assert df.schema is not None # Check that the schema is defined
    assert df.schema.fieldNames() == ["id", "settings"] # Check that the schema has the correct fields
    assert df.schema.fields[0].dataType.simpleString() == "bigint" # Check that the id field is a bigint (LongType)
    assert df.schema.fields[1].dataType.simpleString() == "map<string,string>" # Check that the settings field is a map of string to strings