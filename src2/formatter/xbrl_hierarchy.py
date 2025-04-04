"""
XBRL Hierarchy Extractor

This module extracts and processes hierarchical information from XBRL data.
"""

import re
import logging
from typing import Dict, List, Any, Set, Tuple, Optional


class XBRLHierarchyExtractor:
    """
    Extracts hierarchical information from XBRL data.
    """
    
    def __init__(self):
        """Initialize the XBRL hierarchy extractor."""
        self.logger = logging.getLogger(__name__)
    
    def extract_hierarchy(self, xbrl_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract hierarchical structure from XBRL data.
        
        Args:
            xbrl_data: The raw XBRL data
            
        Returns:
            Dictionary containing the hierarchical structure
        """
        hierarchy = {
            "presentation": {},  # Parent-child relationships
            "calculation": {},   # Calculation relationships
            "statement_mapping": {},  # Map concepts to statement types
            "top_level": {
                "Balance_Sheet": set(),
                "Income_Statement": set(),
                "Cash_Flow_Statement": set(),
                "Statement_Of_Equity": set()
            }
        }
        
        # Process facts to identify statement types and potential top-level concepts
        self._process_facts(xbrl_data, hierarchy)
        
        # Identify parent-child relationships based on concept names
        self._identify_relationships(xbrl_data, hierarchy)
        
        # Identify top-level concepts
        self._identify_top_level_concepts(hierarchy)
        
        return hierarchy
    
    def _process_facts(self, xbrl_data: List[Dict[str, Any]], hierarchy: Dict[str, Any]) -> None:
        """
        Process facts to identify statement types and potential relationships.
        
        Args:
            xbrl_data: The raw XBRL data
            hierarchy: The hierarchy dictionary to update
        """
        # Map of concept patterns to statement types
        concept_patterns = {
            r"(Asset|Liability|Equity|StockholdersEquity)": "Balance_Sheet",
            r"(Revenue|Income|Expense|EarningsPerShare|GrossProfit|OperatingIncome|NetIncome)": "Income_Statement",
            r"(CashFlow|CashAndCashEquivalent|NetCashProvidedByUsedIn)": "Cash_Flow_Statement",
            r"(StockholdersEquity|ShareCapital|RetainedEarnings|AccumulatedOtherComprehensiveIncome)": "Statement_Of_Equity"
        }
        
        # Known top-level concepts
        top_level_concepts = {
            "Balance_Sheet": [
                "us-gaap:Assets", 
                "us-gaap:Liabilities", 
                "us-gaap:StockholdersEquity",
                "us-gaap:LiabilitiesAndStockholdersEquity"
            ],
            "Income_Statement": [
                "us-gaap:Revenues",
                "us-gaap:CostsAndExpenses",
                "us-gaap:OperatingIncomeLoss",
                "us-gaap:NetIncomeLoss"
            ],
            "Cash_Flow_Statement": [
                "us-gaap:NetCashProvidedByUsedInOperatingActivities",
                "us-gaap:NetCashProvidedByUsedInInvestingActivities",
                "us-gaap:NetCashProvidedByUsedInFinancingActivities"
            ],
            "Statement_Of_Equity": [
                "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"
            ]
        }
        
        # Process each fact
        for fact in xbrl_data:
            concept = fact.get("name", "")
            if not concept:
                continue
            
            # Determine statement type
            statement_type = "Unknown"
            for pattern, st_type in concept_patterns.items():
                if re.search(pattern, concept):
                    statement_type = st_type
                    break
            
            # Map concept to statement type
            hierarchy["statement_mapping"][concept] = statement_type
            
            # Check if it's a known top-level concept
            for st_type, concepts in top_level_concepts.items():
                if concept in concepts:
                    hierarchy["top_level"][st_type].add(concept)
    
    def _identify_relationships(self, xbrl_data: List[Dict[str, Any]], hierarchy: Dict[str, Any]) -> None:
        """
        Identify parent-child relationships based on concept names.
        
        Args:
            xbrl_data: The raw XBRL data
            hierarchy: The hierarchy dictionary to update
        """
        # Extract all concept names
        concepts = set()
        for fact in xbrl_data:
            concept = fact.get("name", "")
            if concept:
                concepts.add(concept)
        
        # Identify potential parent-child relationships based on naming patterns
        for concept in concepts:
            # Skip concepts that don't have a statement type
            if concept not in hierarchy["statement_mapping"]:
                continue
                
            statement_type = hierarchy["statement_mapping"][concept]
            
            # Check for potential parent concepts
            for parent_concept in concepts:
                # Skip if same concept or parent doesn't have a statement type
                if parent_concept == concept or parent_concept not in hierarchy["statement_mapping"]:
                    continue
                    
                parent_statement_type = hierarchy["statement_mapping"][parent_concept]
                
                # Skip if different statement types
                if parent_statement_type != statement_type:
                    continue
                
                # Check if concept is a child of parent based on naming patterns
                if self._is_child_of(concept, parent_concept):
                    if parent_concept not in hierarchy["presentation"]:
                        hierarchy["presentation"][parent_concept] = []
                    
                    hierarchy["presentation"][parent_concept].append({
                        "child": concept,
                        "order": len(hierarchy["presentation"][parent_concept])
                    })
    
    def _is_child_of(self, concept: str, parent_concept: str) -> bool:
        """
        Determine if a concept is a child of a parent concept based on naming patterns.
        
        Args:
            concept: The potential child concept
            parent_concept: The potential parent concept
            
        Returns:
            True if concept is likely a child of parent_concept
        """
        # Extract the concept name without the prefix
        concept_name = concept.split(":")[-1]
        parent_name = parent_concept.split(":")[-1]
        
        # Check if parent is a prefix of child
        if concept_name.startswith(parent_name) and concept_name != parent_name:
            return True
        
        # Check for common parent-child patterns
        parent_child_patterns = [
            # Assets and sub-categories
            ("Assets", "AssetsCurrent"),
            ("Assets", "AssetsNoncurrent"),
            ("Assets", "CashAndCashEquivalents"),
            ("Assets", "Inventory"),
            
            # Liabilities and sub-categories
            ("Liabilities", "LiabilitiesCurrent"),
            ("Liabilities", "LiabilitiesNoncurrent"),
            ("Liabilities", "AccountsPayable"),
            ("Liabilities", "DeferredIncomeTaxLiabilities"),
            
            # Equity and sub-categories
            ("StockholdersEquity", "CommonStock"),
            ("StockholdersEquity", "RetainedEarnings"),
            ("StockholdersEquity", "AccumulatedOtherComprehensiveIncome"),
            
            # Income statement items
            ("Revenues", "RevenueFromContractWithCustomer"),
            ("CostsAndExpenses", "CostOfGoodsAndServicesSold"),
            ("CostsAndExpenses", "OperatingExpenses"),
            
            # Cash flow items
            ("NetCashProvidedByUsedInOperatingActivities", "DepreciationAndAmortization"),
            ("NetCashProvidedByUsedInInvestingActivities", "PaymentsToAcquirePropertyPlantAndEquipment")
        ]
        
        for p_pattern, c_pattern in parent_child_patterns:
            if parent_name.endswith(p_pattern) and concept_name.endswith(c_pattern):
                return True
        
        return False
    
    def _identify_top_level_concepts(self, hierarchy: Dict[str, Any]) -> None:
        """
        Identify top-level concepts for each statement type.
        
        Args:
            hierarchy: The hierarchy dictionary to update
        """
        # For each statement type, find concepts that aren't children of any other concept
        for statement_type in ["Balance_Sheet", "Income_Statement", "Cash_Flow_Statement", "Statement_Of_Equity"]:
            # Get all concepts for this statement type
            concepts = set()
            for concept, st in hierarchy["statement_mapping"].items():
                if st == statement_type:
                    concepts.add(concept)
            
            # Get all child concepts
            children = set()
            for parent, child_list in hierarchy["presentation"].items():
                for child_info in child_list:
                    children.add(child_info["child"])
            
            # Top-level concepts are those that aren't children of any other concept
            top_level = concepts - children
            
            # Add to the hierarchy
            hierarchy["top_level"][statement_type].update(top_level)
            
            # If we don't have any top-level concepts, use known ones
            if not hierarchy["top_level"][statement_type]:
                if statement_type == "Balance_Sheet":
                    hierarchy["top_level"][statement_type].update([
                        "us-gaap:Assets", 
                        "us-gaap:Liabilities", 
                        "us-gaap:StockholdersEquity",
                        "us-gaap:LiabilitiesAndStockholdersEquity"
                    ])
                elif statement_type == "Income_Statement":
                    hierarchy["top_level"][statement_type].update([
                        "us-gaap:Revenues",
                        "us-gaap:CostsAndExpenses",
                        "us-gaap:OperatingIncomeLoss",
                        "us-gaap:NetIncomeLoss"
                    ])
                elif statement_type == "Cash_Flow_Statement":
                    hierarchy["top_level"][statement_type].update([
                        "us-gaap:NetCashProvidedByUsedInOperatingActivities",
                        "us-gaap:NetCashProvidedByUsedInInvestingActivities",
                        "us-gaap:NetCashProvidedByUsedInFinancingActivities"
                    ])
                elif statement_type == "Statement_Of_Equity":
                    hierarchy["top_level"][statement_type].update([
                        "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"
                    ])
    
    def get_concept_level(self, concept: str, hierarchy: Dict[str, Any]) -> Tuple[str, int]:
        """
        Determine the statement type and hierarchical level of a concept.
        
        Args:
            concept: The concept to check
            hierarchy: The hierarchy information
            
        Returns:
            Tuple of (statement_type, level)
        """
        # Get statement type
        statement_type = hierarchy["statement_mapping"].get(concept, "Unknown")
        
        # Check if it's a top-level concept
        if statement_type != "Unknown" and concept in hierarchy["top_level"][statement_type]:
            return statement_type, 0
        
        # Check if it's a direct child of a top-level concept
        for parent in hierarchy["presentation"]:
            if parent in hierarchy["top_level"].get(statement_type, set()):
                for child_info in hierarchy["presentation"][parent]:
                    if child_info["child"] == concept:
                        return statement_type, 1
        
        # Default to level 2 (sub-item)
        return statement_type, 2
