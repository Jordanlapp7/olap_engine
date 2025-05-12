use crate::plan::*;
use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn parse_sql_to_plan<'a>(query: &str, table: &'a Table) -> PlanNode<'a> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, query).unwrap();
    let stmt = match &ast[0] {
        Statement::Query(q) => q,
        _ => panic!("Only SELECT queries supported"),
    };

    // Walk through the parsed query and build a PlanNode chain
    // Start with ScanNode
    let mut plan = PlanNode::Scan(ScanNode { table });

    // WHERE FilterNode
    if let Some(selection) = &stmt.body.get_selection() {
        if let Expr::BinaryOp { left, op, right } = selection {
            if let (Expr::Identifier(ident), BinaryOperator::Eq, Expr::Value(Value::SingleQuotedString(val))) = (&**left, op, &**right) {
                plan = PlanNode::Filter(FilterNode {
                    input: Box::new(plan),
                    predicate: Box::new(move |row| row[&ident.value] == *val),
                });
            }
        }
    }

    // GROUP BY AggregateNode
    if !stmt.body.get_group_by().is_empty() {
        let group_by_cols = stmt.body.get_group_by().iter().map(|e| match e {
            Expr::Identifier(ident) => ident.value.clone(),
            _ => panic!("Unsupported group by expression"),
        }).collect();

        let mut aggregates = vec![];

        if let Select { projection, .. } = &stmt.body.get_select() {
            for item in projection {
                if let SelectItem::UnnamedExpr(Expr::Function(func)) = item {
                    let name = func.args[0].to_string().replace('\"', "");
                    let agg = match func.name.to_string().to_lowercase().as_str() {
                        "sum" => AggregateFunction::Sum,
                        "count" => AggregateFunction::Count,
                        "avg" => AggregateFunction::Avg,
                        _ => panic!("Unsupported aggregation"),
                    };
                    aggregates.push((name, agg));
                }
            }
        }

        plan = PlanNode::Aggregate(AggregateNode {
            input: Box::new(plan),
            group_by: group_by_cols,
            aggregates,
        });
    }

    // SELECT ProjectNode
    if let Select { projection, .. } = &stmt.body.get_select() {
        let cols: Vec<String> = projection.iter().filter_map(|item| match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.value.clone()),
            _ => None,
        }).collect();

        if !cols.is_empty() {
            plan = PlanNode::Project(ProjectNode {
                input: Box::new(plan),
                columns: cols,
            });
        }
    }

    plan
}