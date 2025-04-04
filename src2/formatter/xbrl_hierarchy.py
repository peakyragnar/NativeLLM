"""
XBRL Hierarchy Extractor

This module extracts and processes hierarchical information from XBRL data.
It uses a company-agnostic approach to extract relationships directly from XBRL structure.
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
        Extract hierarchical structure from XBRL data using a company-agnostic approach.

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

        # Extract all concepts from the data
        concepts = set()
        for fact in xbrl_data:
            concept = fact.get("name", "")
            if concept:
                concepts.add(concept)

        # First, try to extract relationships from presentation networks if available
        presentation_networks = self._extract_presentation_networks(xbrl_data)
        if presentation_networks:
            self.logger.info(f"Extracted {len(presentation_networks)} presentation networks")
            self._process_presentation_networks(presentation_networks, hierarchy)

        # Next, try to extract relationships from calculation networks if available
        calculation_networks = self._extract_calculation_networks(xbrl_data)
        if calculation_networks:
            self.logger.info(f"Extracted {len(calculation_networks)} calculation networks")
            self._process_calculation_networks(calculation_networks, hierarchy)

        # If we couldn't extract relationships from networks, fall back to pattern-based approach
        if not hierarchy["presentation"]:
            self.logger.info("No presentation networks found, using pattern-based approach")
            self._identify_relationships_from_patterns(concepts, hierarchy)

        # Process facts to identify statement types
        self._process_facts(xbrl_data, hierarchy)

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

    def _extract_presentation_networks(self, xbrl_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract presentation networks from XBRL data.

        Args:
            xbrl_data: The raw XBRL data

        Returns:
            List of presentation networks
        """
        # In raw XBRL data, presentation networks might be embedded in the facts
        # We need to look for specific patterns that indicate presentation relationships
        networks = []

        # Look for presentation arcs in the data
        presentation_arcs = []
        for fact in xbrl_data:
            name = fact.get("name", "")
            if "presentationArc" in name or "presentationLink" in name:
                presentation_arcs.append(fact)

        # If we found presentation arcs, try to extract networks
        if presentation_arcs:
            # Group arcs by role
            arcs_by_role = {}
            for arc in presentation_arcs:
                role = arc.get("role", "")
                if not role:
                    continue

                if role not in arcs_by_role:
                    arcs_by_role[role] = []

                arcs_by_role[role].append(arc)

            # Create networks from arcs
            for role, arcs in arcs_by_role.items():
                network = {
                    "role": role,
                    "links": []
                }

                for arc in arcs:
                    parent = arc.get("from", "")
                    child = arc.get("to", "")
                    order = arc.get("order", 0)

                    if parent and child:
                        network["links"].append({
                            "from": parent,
                            "to": child,
                            "order": order
                        })

                if network["links"]:
                    networks.append(network)

        return networks

    def _extract_calculation_networks(self, xbrl_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract calculation networks from XBRL data.

        Args:
            xbrl_data: The raw XBRL data

        Returns:
            List of calculation networks
        """
        # Similar to presentation networks, but looking for calculation arcs
        networks = []

        # Look for calculation arcs in the data
        calculation_arcs = []
        for fact in xbrl_data:
            name = fact.get("name", "")
            if "calculationArc" in name or "calculationLink" in name:
                calculation_arcs.append(fact)

        # If we found calculation arcs, try to extract networks
        if calculation_arcs:
            # Group arcs by role
            arcs_by_role = {}
            for arc in calculation_arcs:
                role = arc.get("role", "")
                if not role:
                    continue

                if role not in arcs_by_role:
                    arcs_by_role[role] = []

                arcs_by_role[role].append(arc)

            # Create networks from arcs
            for role, arcs in arcs_by_role.items():
                network = {
                    "role": role,
                    "links": []
                }

                for arc in arcs:
                    parent = arc.get("from", "")
                    child = arc.get("to", "")
                    weight = arc.get("weight", 1.0)

                    if parent and child:
                        network["links"].append({
                            "from": parent,
                            "to": child,
                            "weight": weight
                        })

                if network["links"]:
                    networks.append(network)

        return networks

    def _process_presentation_networks(self, networks: List[Dict[str, Any]], hierarchy: Dict[str, Any]) -> None:
        """
        Process presentation networks to update hierarchy.

        Args:
            networks: List of presentation networks
            hierarchy: The hierarchy dictionary to update
        """
        for network in networks:
            role = network.get("role", "")
            statement_type = self._determine_statement_type_from_role(role)

            for link in network.get("links", []):
                parent = link.get("from", "")
                child = link.get("to", "")
                order = link.get("order", 0)

                if parent and child:
                    if parent not in hierarchy["presentation"]:
                        hierarchy["presentation"][parent] = []

                    hierarchy["presentation"][parent].append({
                        "child": child,
                        "order": order
                    })

                    # Map concepts to statement type
                    hierarchy["statement_mapping"][parent] = statement_type
                    hierarchy["statement_mapping"][child] = statement_type

    def _process_calculation_networks(self, networks: List[Dict[str, Any]], hierarchy: Dict[str, Any]) -> None:
        """
        Process calculation networks to update hierarchy.

        Args:
            networks: List of calculation networks
            hierarchy: The hierarchy dictionary to update
        """
        for network in networks:
            role = network.get("role", "")

            for link in network.get("links", []):
                parent = link.get("from", "")
                child = link.get("to", "")
                weight = link.get("weight", 1.0)

                if parent and child:
                    if parent not in hierarchy["calculation"]:
                        hierarchy["calculation"][parent] = []

                    hierarchy["calculation"][parent].append({
                        "child": child,
                        "weight": weight
                    })

    def _determine_statement_type_from_role(self, role: str) -> str:
        """
        Determine statement type from role URI.

        Args:
            role: The role URI

        Returns:
            The statement type
        """
        role_lower = role.lower()

        if any(term in role_lower for term in ["balance", "financial position", "statement of financial position"]):
            return "Balance_Sheet"
        elif any(term in role_lower for term in ["income", "operations", "profit", "loss", "comprehensive income"]):
            return "Income_Statement"
        elif any(term in role_lower for term in ["cash flow", "cashflow", "statement of cash flows"]):
            return "Cash_Flow_Statement"
        elif any(term in role_lower for term in ["equity", "stockholder", "shareholder", "changes in equity"]):
            return "Statement_Of_Equity"
        else:
            return "Unknown"

    def _identify_relationships_from_patterns(self, concepts: Set[str], hierarchy: Dict[str, Any]) -> None:
        """
        Identify relationships based on concept naming patterns.

        Args:
            concepts: Set of all concept names
            hierarchy: The hierarchy dictionary to update
        """
        # Common patterns across companies
        patterns = [
            # Assets patterns
            ("Assets", ["AssetsCurrent", "AssetsNoncurrent"]),
            ("AssetsCurrent", ["CashAndCashEquivalents", "ShortTermInvestments", "AccountsReceivable", "Inventory"]),
            ("AssetsNoncurrent", ["PropertyPlantAndEquipment", "Goodwill", "IntangibleAssets"]),

            # Liabilities patterns
            ("Liabilities", ["LiabilitiesCurrent", "LiabilitiesNoncurrent"]),
            ("LiabilitiesCurrent", ["AccountsPayable", "AccruedLiabilities", "CustomerDeposits", "DeferredRevenue"]),
            ("LiabilitiesNoncurrent", ["LongTermDebt", "DeferredTaxLiabilities", "LeaseLiability"]),

            # Equity patterns
            ("StockholdersEquity", ["CommonStock", "AdditionalPaidInCapital", "RetainedEarnings", "AccumulatedOtherComprehensiveIncome"]),

            # Income statement patterns
            ("Revenues", ["RevenueFromContractWithCustomer", "InterestIncome", "OtherIncome"]),
            ("CostsAndExpenses", ["CostOfGoodsAndServicesSold", "ResearchAndDevelopmentExpense", "SellingGeneralAndAdministrativeExpense"])
        ]

        for parent_suffix, child_suffixes in patterns:
            # Find all concepts that end with the parent suffix
            parent_concepts = [c for c in concepts if c.split(":")[-1].endswith(parent_suffix)]

            for parent in parent_concepts:
                if parent not in hierarchy["presentation"]:
                    hierarchy["presentation"][parent] = []

                # Find all concepts that end with any of the child suffixes
                for child_suffix in child_suffixes:
                    child_concepts = [c for c in concepts if c.split(":")[-1].endswith(child_suffix)]

                    for child in child_concepts:
                        if child != parent:  # Avoid self-references
                            hierarchy["presentation"][parent].append({
                                "child": child,
                                "order": len(hierarchy["presentation"][parent])
                            })

                            # Determine statement type based on naming patterns
                            statement_type = self._determine_statement_type_from_name(parent)
                            hierarchy["statement_mapping"][parent] = statement_type
                            hierarchy["statement_mapping"][child] = statement_type

    def _determine_statement_type_from_name(self, concept: str) -> str:
        """
        Determine statement type from concept name.

        Args:
            concept: The concept name

        Returns:
            The statement type
        """
        concept_name = concept.split(":")[-1]

        if any(pattern in concept_name for pattern in ["Asset", "Liability", "Equity", "StockholdersEquity"]):
            return "Balance_Sheet"
        elif any(pattern in concept_name for pattern in ["Revenue", "Income", "Expense", "EarningsPerShare", "GrossProfit", "OperatingIncome", "NetIncome"]):
            return "Income_Statement"
        elif any(pattern in concept_name for pattern in ["CashFlow", "CashAndCashEquivalent", "NetCashProvidedByUsedIn"]):
            return "Cash_Flow_Statement"
        elif any(pattern in concept_name for pattern in ["StockholdersEquity", "ShareCapital", "RetainedEarnings", "AccumulatedOtherComprehensiveIncome"]):
            return "Statement_Of_Equity"
        else:
            return "Unknown"

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

        # If statement type is unknown, try to determine it from the concept name
        if statement_type == "Unknown":
            statement_type = self._determine_statement_type_from_name(concept)

        # Check if it's a top-level concept
        if statement_type != "Unknown" and concept in hierarchy["top_level"][statement_type]:
            return statement_type, 0

        # Check if it's a direct child of a top-level concept
        for parent in hierarchy["presentation"]:
            if parent in hierarchy["top_level"].get(statement_type, set()):
                for child_info in hierarchy["presentation"][parent]:
                    if child_info["child"] == concept:
                        return statement_type, 1

        # Check if it's a child of any concept (level 2)
        for parent in hierarchy["presentation"]:
            for child_info in hierarchy["presentation"][parent]:
                if child_info["child"] == concept:
                    # Check if the parent is a level 1 concept
                    parent_statement_type, parent_level = self.get_concept_level(parent, hierarchy)
                    if parent_level == 1:
                        return statement_type, 2

        # Check if it's a parent of any concept
        if concept in hierarchy["presentation"]:
            # If it has children but isn't a top-level concept, it's likely level 1
            return statement_type, 1

        # Default to level 2 (sub-item)
        return statement_type, 2
