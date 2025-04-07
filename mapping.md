# XBRL Hierarchy Mapping Approach

This document outlines our approach to extracting and validating hierarchical relationships from XBRL documents, including the sources of mapping definitions and our confidence levels in different approaches.

## 1. Sources of XBRL Mapping Definitions

XBRL mapping definitions come from several authoritative sources, listed here in order of reliability:

### 1.1 Official XBRL Taxonomies

- **US GAAP Taxonomy**: Published by the Financial Accounting Standards Board (FASB)
  - Defines standard concepts and relationships for US GAAP financial reporting
  - Updated annually to reflect changes in accounting standards
  - Available at: https://xbrl.fasb.org/

- **IFRS Taxonomy**: Published by the International Accounting Standards Board (IASB)
  - Defines standard concepts and relationships for IFRS financial reporting
  - Updated annually to reflect changes in accounting standards
  - Available at: https://www.ifrs.org/issued-standards/ifrs-taxonomy/

- **SEC Reporting Taxonomy**: Extensions specific to SEC filing requirements
  - Defines additional concepts and relationships required by the SEC
  - Integrated with the US GAAP Taxonomy
  - Available at: https://www.sec.gov/info/edgar/edgartaxonomies.shtml

### 1.2 Linkbase Files in XBRL Documents

- **Presentation Linkbases** (`*_pre.xml`):
  - Define parent-child relationships for display purposes
  - Represent the hierarchical structure of financial statements
  - Example: Assets → Current Assets → Cash and Cash Equivalents

- **Calculation Linkbases** (`*_cal.xml`):
  - Define mathematical relationships between concepts
  - Specify how values should be summed or calculated
  - Example: Current Assets = Cash + Short-term Investments + Accounts Receivable + Inventory

- **Definition Linkbases** (`*_def.xml`):
  - Define dimensional relationships and other concept relationships
  - Specify how concepts relate to dimensions and domains
  - Example: Revenue [by Segment] [by Geography]

- **Label Linkbases** (`*_lab.xml`):
  - Define human-readable labels for concepts
  - Provide different labels for different purposes (terse, verbose, etc.)
  - Example: "PropertyPlantAndEquipmentNet" → "Property, Plant and Equipment, Net"

### 1.3 Company-Specific Extensions

- Companies can extend the standard taxonomies with their own concepts and relationships
- These extensions are defined in the company's schema file (`.xsd`)
- Extensions must follow XBRL rules and be consistent with the base taxonomy

## 2. Extraction Approaches

Our extraction approach follows a hierarchical strategy, starting with the most reliable sources and falling back to less reliable sources when necessary:

### 2.1 Direct Extraction from Linkbase Files

```python
def process_presentation_linkbase(linkbase_soup):
    """Process a presentation linkbase file."""
    presentation_links = linkbase_soup.find_all('link:presentationLink')
    for link in presentation_links:
        role = link.get('xlink:role')
        arcs = link.find_all('link:presentationArc')
        for arc in arcs:
            from_attr = arc.get('xlink:from')
            to_attr = arc.get('xlink:to')
            # Extract relationships from locators
            # ...
```

- **Confidence Level**: Very High
- **Basis for Confidence**: These relationships are defined by the company, validated by the SEC, and follow XBRL standards.
- **Validation Method**: Cross-check with calculation linkbases and financial statement structure.

### 2.2 Extraction from Embedded Linkbases

```python
def extract_embedded_linkbases(schema_soup):
    """Extract embedded linkbases from schema file."""
    linkbase_elements = schema_soup.find_all('link:linkbase')
    for linkbase in linkbase_elements:
        presentation_links = linkbase.find_all('link:presentationLink')
        for link in presentation_links:
            # Process presentation links
            # ...
```

- **Confidence Level**: High
- **Basis for Confidence**: These relationships are defined by the company and validated by the SEC, but the extraction process is more complex.
- **Validation Method**: Cross-check with calculation linkbases and financial statement structure.

### 2.3 Multiple Resolution Strategies

For complex or non-standard XBRL documents, we employ multiple resolution strategies:

#### 2.3.1 Standard Locator Resolution

```python
def resolve_standard_locators(from_attr, to_attr, linkbase):
    """Standard locator resolution using link:loc elements."""
    loc_elements = linkbase.find_all(['link:loc', 'loc'])
    from_loc = next((loc for loc in loc_elements if loc.get('xlink:label') == from_attr), None)
    to_loc = next((loc for loc in loc_elements if loc.get('xlink:label') == to_attr), None)
    # Extract concepts from hrefs
    # ...
```

- **Confidence Level**: High
- **Basis for Confidence**: This is the standard XBRL approach for resolving relationships.

#### 2.3.2 Direct Concept References

```python
def resolve_direct_concepts(from_attr, to_attr):
    """Direct concept resolution (used by some companies)."""
    if from_attr in self.concepts:
        from_concept = from_attr
    if to_attr in self.concepts:
        to_concept = to_attr
    # ...
```

- **Confidence Level**: Medium-High
- **Basis for Confidence**: This approach works for companies that use direct concept references, but requires validation.

#### 2.3.3 Namespace-Aware Matching

```python
def resolve_namespace_concepts(from_attr, to_attr):
    """Resolve concepts using namespace-aware matching."""
    from_name = from_attr.split(':')[-1]
    to_name = to_attr.split(':')[-1]
    for concept in self.concepts:
        concept_name = concept.split(':')[-1]
        if concept_name == from_name:
            from_concept = concept
        # ...
```

- **Confidence Level**: Medium
- **Basis for Confidence**: This approach handles namespace variations but may produce false matches.

### 2.4 Pattern Matching (Fallback)

```python
def apply_pattern_matching():
    """Apply pattern matching to identify relationships when other methods fail."""
    patterns = [
        ("Assets", ["AssetsCurrent", "AssetsNoncurrent"]),
        ("AssetsCurrent", ["CashAndCashEquivalents", "ShortTermInvestments"]),
        # ...
    ]
    # Apply patterns to concepts
    # ...
```

- **Confidence Level**: Medium-Low
- **Basis for Confidence**: This approach is based on standard financial statement structures but doesn't use company-specific relationships.
- **Validation Method**: Cross-check with financial statement structure in the HTML document.

## 3. Validation Approaches

To ensure the correctness of our extracted hierarchies, we employ several validation approaches:

### 3.1 Cross-Validation with Multiple Sources

- Compare relationships extracted from different linkbases (presentation, calculation, definition)
- Compare relationships across multiple filings from the same company
- Compare relationships with standard taxonomy relationships

### 3.2 Validation Against Financial Statement Structure

- Balance sheets must balance (Assets = Liabilities + Equity)
- Income statements follow a standard structure (Revenue - Expenses = Net Income)
- Cash flow statements categorize flows into operating, investing, and financing activities

### 3.3 Concept Label Analysis

- Use concept labels to validate relationships
- Labels often contain hierarchical information (e.g., "Current Assets: Cash and Cash Equivalents")
- Compare labels across concepts to identify potential relationships

### 3.4 Manual Validation

- For critical financial statements, perform manual validation against the HTML document
- Verify that the extracted hierarchy matches the structure in the HTML document
- Identify and correct any discrepancies

## 4. Confidence Levels and Decision Making

Our approach to using extracted hierarchies is based on confidence levels:

### 4.1 Very High Confidence (Direct Linkbase Extraction)

- Use these relationships without additional validation
- Rely on these relationships for critical financial analysis
- Example: Relationships extracted directly from presentation linkbases

### 4.2 High Confidence (Embedded Linkbases, Standard Resolution)

- Use these relationships with minimal additional validation
- Cross-check with calculation linkbases for consistency
- Example: Relationships extracted from embedded linkbases in schema files

### 4.3 Medium Confidence (Namespace Matching, Direct References)

- Use these relationships with additional validation
- Cross-check with financial statement structure in the HTML document
- Example: Relationships resolved using namespace-aware matching

### 4.4 Low Confidence (Pattern Matching, Fallbacks)

- Use these relationships only when higher-confidence sources are unavailable
- Validate against the HTML document and other sources
- Flag these relationships as potentially unreliable
- Example: Relationships created through pattern matching

## 5. Implementation Guidelines

When implementing XBRL hierarchy extraction, follow these guidelines:

### 5.1 Prioritize Authoritative Sources

1. Direct extraction from linkbase files (highest confidence)
2. Extraction from embedded linkbases in schema files (high confidence)
3. Resolution using multiple strategies (medium confidence)
4. Pattern matching based on concept names and labels (low confidence)

### 5.2 Document Confidence Levels

- Include confidence levels in the extracted hierarchy
- Allow downstream processes to make decisions based on confidence levels
- Example: `{'relationship': {...}, 'confidence': 'high'}`

### 5.3 Provide Fallback Mechanisms

- Always have a fallback mechanism for when authoritative sources fail
- Use pattern matching as a last resort, not as a primary source
- Document when fallback mechanisms are used

### 5.4 Validate Results

- Validate extracted hierarchies against known financial statement structures
- Cross-check with multiple sources when possible
- Document validation results and any discrepancies

## 6. Future Improvements

To improve our XBRL hierarchy extraction approach, consider these future enhancements:

### 6.1 Machine Learning for Pattern Recognition

- Train models to recognize financial statement patterns
- Use historical data to improve pattern matching
- Incorporate feedback from manual validation

### 6.2 Enhanced Taxonomy Integration

- Integrate more deeply with standard taxonomies (US GAAP, IFRS)
- Use taxonomy relationships as a fallback when company-specific relationships are unavailable
- Keep taxonomies up-to-date with annual releases

### 6.3 Company-Specific Learning

- Learn and store company-specific patterns over time
- Build company-specific models for relationship extraction
- Use historical filings to improve extraction for future filings

## 7. Conclusion

XBRL hierarchy extraction is a complex process that requires a multi-layered approach. By prioritizing authoritative sources, providing fallback mechanisms, and validating results, we can extract hierarchical relationships with high confidence. This approach ensures that our financial analysis is based on reliable and accurate data.

By following the guidelines in this document, we can extract XBRL hierarchies that accurately represent the structure of financial statements, enabling more accurate and reliable financial analysis.
