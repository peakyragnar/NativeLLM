#!/bin/bash

# This script fixes the XBRL data structure in the pipeline.py file

# Back up the original file
cp /Users/michael/NativeLLM/src2/sec/pipeline.py /Users/michael/NativeLLM/src2/sec/pipeline.py.backup

# Create a new file with the updated code
cat > /Users/michael/NativeLLM/fix_xbrl_data.py << 'EOF'
# Helper function to add a fact
def add_fact(xbrl_data, concept, value, context_ref="AsOf", unit_ref=None, decimals=None):
    if context_ref not in xbrl_data["facts"]:
        xbrl_data["facts"][context_ref] = {}
    if concept not in xbrl_data["facts"][context_ref]:
        xbrl_data["facts"][context_ref][concept] = []
    
    fact_data = {"value": value}
    if unit_ref:
        fact_data["unit"] = unit_ref
    if decimals:
        fact_data["decimals"] = decimals
    
    xbrl_data["facts"][context_ref][concept].append(fact_data)
EOF

# Update the pipeline.py file
awk '
/# Initialize XBRL data structure/ {
    print;
    getline;
    print "                    xbrl_data = {";
    print "                        \"contexts\": {},";
    print "                        \"units\": {},";
    print "                        \"facts\": {},  # Use a dictionary instead of a list";
    print "                        \"concepts\": {}  # Add concepts as expected by formatter";
    print "                    }";
    print "                    ";
    print "                    # Create default context for basic facts";
    print "                    xbrl_data[\"contexts\"][\"AsOf\"] = {";
    print "                        \"entity\": {\"identifier\": metadata.get(\"ticker\", \"\")},";
    print "                        \"period\": {\"instant\": metadata.get(\"period_end_date\", \"\")}";
    print "                    }";
    print "                    ";
    print "                    # Set up facts dictionary structure";
    print "                    # Helper function to add a fact";
    print "                    def add_fact(concept, value, context_ref=\"AsOf\", unit_ref=None, decimals=None):";
    print "                        if context_ref not in xbrl_data[\"facts\"]:";
    print "                            xbrl_data[\"facts\"][context_ref] = {}";
    print "                        if concept not in xbrl_data[\"facts\"][context_ref]:";
    print "                            xbrl_data[\"facts\"][context_ref][concept] = []";
    print "                        ";
    print "                        fact_data = {\"value\": value}";
    print "                        if unit_ref:";
    print "                            fact_data[\"unit\"] = unit_ref";
    print "                        if decimals:";
    print "                            fact_data[\"decimals\"] = decimals";
    print "                        ";
    print "                        xbrl_data[\"facts\"][context_ref][concept].append(fact_data)";
    print "                    ";
    # Skip the original XBRL data structure definition lines
    for (i=1; i<=3; i++) {
        getline;
    }
}

/# Start with basic document information/ {
    print;
    getline;
    # Replace the original xbrl_data["facts"].append with add_fact calls
    print "                    # Add document type";
    print "                    add_fact(\"DocumentType\", filing_type)";
    print "                    # Add company name";
    print "                    add_fact(\"EntityRegistrantName\", metadata.get(\"company_name\", \"\"))";
    # Skip the original fact append lines
    for (i=1; i<=7; i++) {
        getline;
    }
}

/for ix_tag in ix_tags:/ {
    print;
    getline;
    # Replace the original nonfraction extraction with the new format
    print "                                if ix_tag.name == \"ix:nonfraction\":";
    print "                                    concept = ix_tag.get(\"name\", \"\")";
    print "                                    value = ix_tag.text.strip()";
    print "                                    context_ref = ix_tag.get(\"contextref\", \"\")";
    print "                                    unit_ref = ix_tag.get(\"unitref\", None)";
    print "                                    decimals = ix_tag.get(\"decimals\", None)";
    print "                                    ";
    print "                                    # Add the fact using the helper function";
    print "                                    add_fact(concept, value, context_ref, unit_ref, decimals)";
    # Skip the original fact extraction and append code
    for (i=1; i<=11; i++) {
        getline;
    }
}

# Print all other lines unchanged
{ print }
' /Users/michael/NativeLLM/src2/sec/pipeline.py.backup > /Users/michael/NativeLLM/src2/sec/pipeline.py.fixed

# Replace the original file with the fixed version if the fixed file exists and isn't empty
if [ -s /Users/michael/NativeLLM/src2/sec/pipeline.py.fixed ]; then
    mv /Users/michael/NativeLLM/src2/sec/pipeline.py.fixed /Users/michael/NativeLLM/src2/sec/pipeline.py
    echo "Successfully updated pipeline.py to fix XBRL data structure"
else
    echo "Error: Failed to create fixed pipeline.py file"
fi