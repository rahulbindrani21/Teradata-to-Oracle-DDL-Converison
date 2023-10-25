import re


def convert_teradata_ddl_to_oracle(ddl):

    # get table name
    table_name = re.findall(r"CREATE.*TABLE\s+(\S+)\.(\S+)\s+,", ddl)

    # convert <database>.<tablename> to <database>_<tablename>
    table_name = table_name[0][0]+"_"+table_name[0][1]
    # print(table_name)

    # remove SET/MULTISET and convert <database>.<tablename> to <database>_<tablename>
    ddl = re.sub(r"CREATE.*TABLE\s+(\S+)\.(\S+)\s+,", r"CREATE TABLE \1_\2,", ddl)

    # Remove fallback and journaling clauses
    ddl = re.sub(r",\s*NO\s+FALLBACK\s*,?\s*", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r",\s*FALLBACK\s*,?\s*", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"\s*NO\s+BEFORE\s+JOURNAL\s*,?\s*", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"\s*NO\s+AFTER\s+JOURNAL\s*,?\s*", "", ddl, flags=re.IGNORECASE)

    # Remove checksum clause
    ddl = re.sub(r"\s*CHECKSUM\s*=\s*DEFAULT\s*,?\s*", "", ddl, flags=re.IGNORECASE)

    # Replace DEFAULT MERGEBLOCKRATIO with (
    ddl = re.sub(r"DEFAULT\s+MERGEBLOCKRATIO\s*", "", ddl, flags=re.IGNORECASE)

    # Replace BYTEINT with NUMBER(3)
    ddl = re.sub(r"BYTEINT", "NUMBER(3)", ddl, flags=re.IGNORECASE)

    # Remove FORMAT clause for DATE columns
    ddl = re.sub(r"\s+TIME\(([^)])\)", r" TIMESTAMP(\1)", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"\s+FORMAT\s+'[^']*'\s*", " ", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"\s+FORMAT\s+[\'\dA-Za-x\/:.-]+\s*", " ", ddl, flags=re.IGNORECASE)

    # Replace INTEGER with NUMBER(10)
    ddl = re.sub(r"INTEGER", "NUMBER(10)", ddl, flags=re.IGNORECASE)

    # Replace BIGINT with NUMBER(10)
    ddl = re.sub(r"BIGINT", "NUMBER(19)", ddl, flags=re.IGNORECASE)

    # Replace DECIMAL with NUMBER
    ddl = re.sub(r"DECIMAL", "NUMBER", ddl, flags=re.IGNORECASE)

    # Replace VARCHAR with VARCHAR2
    ddl = re.sub(r"VARCHAR", "VARCHAR2", ddl, flags=re.IGNORECASE)

    # Replace CHAR with VARCHAR2
    ddl = re.sub(r"\s+CHAR\((\d+)\)", r" VARCHAR2(\1)", ddl, flags=re.IGNORECASE)

    # Replace CHARACTER SET LATIN NOT CASESPECIFIC with VARCHAR2
    ddl = re.sub(r"CHARACTER\s+SET\s+.*\s+NOT\s+CASESPECIFIC", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"CHARACTER\s+SET\s+.*\s+CASESPECIFIC", "", ddl, flags=re.IGNORECASE)

    # Remove NOT NULL keyword -> optional
    ddl = re.sub(r"NOT NULL", " ", ddl, flags=re.IGNORECASE)

    # Remove COMPRESS keyword
    ddl = re.sub(r"COMPRESS\s+\(([^)]+,*[^)]*)\)", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"COMPRESS.*NOT NULL", " ", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"COMPRESS\s+[^),]*", "", ddl, flags=re.IGNORECASE)

    # REMOVE TITLE
    ddl = re.sub(r"\s+TITLE\s+\'[^,]+\'", " ", ddl, flags=re.IGNORECASE)

    # REORDER NOT NULL DEFAULT
    ddl = re.sub(r"NOT NULL DEFAULT\s+CURRENT_TIMESTAMP\((\d+)\)", r"DEFAULT CURRENT_TIMESTAMP(\1) NOT NULL", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"NOT NULL DEFAULT\s+([^)|^,]+)", r'DEFAULT \1 NOT NULL', ddl, flags=re.IGNORECASE)

    # DEFAULT DATE FORMAT
    ddl = re.sub(r"DEFAULT\s+DATE\s+'([^']*)'\s*", r"DEFAULT TO_DATE('\1', 'YYYY-MM-DD') ", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"DEFAULT\s+DATE\s+NOT\s+NULL", r"NOT NULL ", ddl, flags=re.IGNORECASE)

    # Remove semicolon at the end
    ddl = ddl.strip().rstrip(";")
    unique_primary_index_regex = re.findall(r"UNIQUE PRIMARY INDEX\s*(\w*)\s*\(([^)]+,*[^)]*)\)", ddl)
    primary_index_regex = re.findall(r"PRIMARY INDEX\s*(\w*)\s*\(([^)]+,*[^)]*)\)", ddl)

    # Add Primary key constraint - CHECK THE NAMING CONVENTION (PK_<TABLE_NAME>)
    ddl_split = []
    if unique_primary_index_regex:
        # print(unique_primary_index_regex)
        ddl_split = ddl.rsplit(")\nUNIQUE PRIMARY INDEX", 1)
        ddl = ddl_split[0]
        unique_primary_index_name = "PK_" + table_name
        if unique_primary_index_regex[0][0] != "":
            unique_primary_index_name = unique_primary_index_regex[0][0]
        ddl += f", \n\tCONSTRAINT {unique_primary_index_name} PRIMARY KEY ({unique_primary_index_regex[0][1]})"
    elif primary_index_regex:
        # print(primary_index_regex)
        ddl_split = ddl.rsplit(")\nPRIMARY INDEX", 1)
        ddl = ddl_split[0]
        primary_index_name = "PK_" + table_name
        if primary_index_regex[0][0] != "":
            primary_index_name = primary_index_regex[0][0]
        ddl += f", \n\tCONSTRAINT {primary_index_name} PRIMARY KEY ({primary_index_regex[0][1]})"

    ddl += "\n);"

    # find partition columns
    # print(ddl_split[1])
    # partition_columns = re.search(r"PARTITION BY\s*\(\s*(.*)\s*\)\s*\)\nINDEX", ddl_split[1], re. DOTALL)
    # print(partition_columns)
    # if partition_columns:
    #     partition_columns = partition_columns.group(1).split(",")
    #     partition_columns = [f"{col.strip()} VARCHAR2(50)" for col in partition_columns]
    #     partition_columns = "\n\t\t, ".join(partition_columns)
    #     partition_clause = f"\nPARTITION BY LIST ({partition_columns})\n\t("
    #     partition_count = re.search(r"NO\s+OF\s+PARTITIONS\s+(\d+)", ddl_split[1])
    #     if partition_count:
    #         partition_clause += f"\n\t\t{partition_count.group(1)}\n\t)"
    #     else:
    #         partition_clause += ")\n\tAS\n\tSELECT 1\n\tFROM dual\n\tWHERE 1 = 0"
    # else:
    #     partition_clause = ""
    # print(partition_clause)

    # CREATE INDEX
    if len(ddl_split) > 1:
        index_regex = re.findall(r"\n([^\n,\s)]*)\s*INDEX\s*(\w*)\s*\(([^)]+,*[^)]*)\)", ddl_split[1])
        count = 1
        if index_regex:
            for unique, index_nm, index_col in index_regex:
                index_name = "IDX" + str(count) + "_" + table_name
                # if index_nm != "":
                #     index_name = index_nm
                ddl += f"\nCREATE {unique} INDEX {index_name} ON {table_name} ({index_col});"
                count += 1

    return ddl


# input and output file paths
input_file_path = input("Enter the input file:")
output_file_path = input("Enter the output file:")


# function to split ddls
def split_file(filename):
    with open(filename, 'r') as f:
        content = f.read()
        content = re.split(";", content)
    return content


# main
split_content = split_file(input_file_path)
output_oracle_ddls = []
count = 0
for i in range(len(split_content)-1):
    count += 1
    oracle_ddl = convert_teradata_ddl_to_oracle(split_content[i])
    output_oracle_ddls.append(oracle_ddl)

# write list of converted ddls into file
with open(output_file_path, 'w') as f:
    # Write list to file
    f.writelines("%s\n\n" % item for item in output_oracle_ddls)
print(count)