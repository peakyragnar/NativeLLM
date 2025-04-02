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
