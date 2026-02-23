import unittest
from app.query.parser import parse_query, Operator, QueryAST, Condition


class TestQueryParser(unittest.TestCase):
    def test_empty_query(self):
        ast = parse_query("")
        self.assertEqual(len(ast.conditions), 0)

    def test_single_condition(self):
        ast = parse_query('tenant == "demo"')
        self.assertEqual(len(ast.conditions), 1)
        self.assertEqual(ast.conditions[0].field, "tenant")
        self.assertEqual(ast.conditions[0].operator, Operator.EQ)
        self.assertEqual(ast.conditions[0].value, "demo")

    def test_multiple_conditions(self):
        q = 'tenant == "demo" AND node == "fake_llm_call" AND status == "failure" AND time > "2026-01-01"'
        ast = parse_query(q)
        self.assertEqual(len(ast.conditions), 4)

        self.assertEqual(ast.conditions[0].field, "tenant")
        self.assertEqual(ast.conditions[0].operator, Operator.EQ)
        self.assertEqual(ast.conditions[0].value, "demo")

        self.assertEqual(ast.conditions[1].field, "node")
        self.assertEqual(ast.conditions[1].operator, Operator.EQ)
        self.assertEqual(ast.conditions[1].value, "fake_llm_call")

        self.assertEqual(ast.conditions[2].field, "status")
        self.assertEqual(ast.conditions[2].operator, Operator.EQ)
        self.assertEqual(ast.conditions[2].value, "failure")

        self.assertEqual(ast.conditions[3].field, "time")
        self.assertEqual(ast.conditions[3].operator, Operator.GT)
        self.assertEqual(ast.conditions[3].value, "2026-01-01")

    def test_different_operators(self):
        ast = parse_query("count != 5 AND value < 10.5")
        self.assertEqual(len(ast.conditions), 2)
        self.assertEqual(ast.conditions[0].operator, Operator.NEQ)
        self.assertEqual(ast.conditions[0].value, "5")
        self.assertEqual(ast.conditions[1].operator, Operator.LT)
        self.assertEqual(ast.conditions[1].value, "10.5")

    def test_invalid_syntax_missing_operator(self):
        with self.assertRaises(ValueError) as ctx:
            parse_query('tenant "demo"')
        self.assertTrue("No valid operator found" in str(ctx.exception))

    def test_invalid_syntax_missing_field(self):
        with self.assertRaises(ValueError) as ctx:
            parse_query('== "demo"')
        self.assertTrue("Missing field" in str(ctx.exception))

    def test_invalid_syntax_missing_value(self):
        with self.assertRaises(ValueError) as ctx:
            parse_query("tenant == ")
        self.assertTrue("Missing value" in str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
