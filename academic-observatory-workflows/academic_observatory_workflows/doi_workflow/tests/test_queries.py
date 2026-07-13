import os
import unittest

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.doi_workflow.queries import get_ancestors, ror_to_ror_hierarchy_index
from observatory_platform.files import load_jsonl

FIXTURES_FOLDER = project_path("doi_workflow", "tests", "fixtures")


class TestGetAncestors(unittest.TestCase):
    def test_no_parents(self):
        index = {"a": set()}
        self.assertEqual(get_ancestors(index, "a", {}, set()), set())

    def test_single_parent_chain(self):
        """a -> b -> c -> d (d has no parents)"""
        index = {
            "a": {"b"},
            "b": {"c"},
            "c": {"d"},
            "d": set(),
        }
        self.assertEqual(get_ancestors(index, "a", {}, set()), {"b", "c", "d"})

    def test_diamond_shared_parent(self):
        """a -> {b, c}; b -> d; c -> d; d has no parents"""
        index = {
            "a": {"b", "c"},
            "b": {"d"},
            "c": {"d"},
            "d": set(),
        }
        self.assertEqual(get_ancestors(index, "a", {}, set()), {"b", "c", "d"})

    def test_multiple_parents_multiple_ancestors(self):
        index = {
            "a": {"b", "c"},
            "b": {"x"},
            "c": {"y"},
            "x": set(),
            "y": set(),
        }
        self.assertEqual(get_ancestors(index, "a", {}, set()), {"b", "c", "x", "y"})

    def test_two_node_cycle_does_not_recurse_forever(self):
        """a -> b -> a"""
        index = {"a": {"b"}, "b": {"a"}}
        # Should terminate and not raise RecursionError
        result = get_ancestors(index, "a", {}, set())
        self.assertIsInstance(result, set)

    def test_three_node_cycle_does_not_recurse_forever(self):
        """a -> b -> c -> a"""
        index = {"a": {"b"}, "b": {"c"}, "c": {"a"}}
        result = get_ancestors(index, "a", {}, set())
        self.assertIsInstance(result, set)
        # every node in the cycle should still show up as an ancestor
        self.assertIn(result, [{"a", "b", "c"}, {"b", "c"}])


class TestRorHierarchyIndex(unittest.TestCase):
    def test_ror_to_ror_hierarchy_index(self):
        """Test ror_to_ror_hierarchy_index. Check that correct ancestor relationships created."""

        ror = load_jsonl(os.path.join(FIXTURES_FOLDER, "ror.jsonl"))
        index = ror_to_ror_hierarchy_index(ror)
        self.assertEqual(289, len(index))

        # Auckland
        self.assertEqual(0, len(index["https://ror.org/03b94tp07"]))

        # Curtin
        self.assertEqual(0, len(index["https://ror.org/02n415q13"]))

        # International Centre for Radio Astronomy Research
        self.assertEqual({"https://ror.org/02n415q13", "https://ror.org/047272k79"}, index["https://ror.org/05sd1pp77"])
