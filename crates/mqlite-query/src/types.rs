use bson::{Bson, Document};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitTestMode {
    AllSet,
    AllClear,
    AnySet,
    AnyClear,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeSet {
    pub all_numbers: bool,
    pub codes: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MatchExpr {
    AlwaysFalse,
    AlwaysTrue,
    And(Vec<MatchExpr>),
    Or(Vec<MatchExpr>),
    Nor(Vec<MatchExpr>),
    Not(Box<MatchExpr>),
    Expr(Bson),
    Eq {
        path: String,
        value: Bson,
    },
    Ne {
        path: String,
        value: Bson,
    },
    Gt {
        path: String,
        value: Bson,
    },
    Gte {
        path: String,
        value: Bson,
    },
    Lt {
        path: String,
        value: Bson,
    },
    Lte {
        path: String,
        value: Bson,
    },
    In {
        path: String,
        values: Vec<Bson>,
    },
    Nin {
        path: String,
        values: Vec<Bson>,
    },
    All {
        path: String,
        values: Vec<Bson>,
    },
    Exists {
        path: String,
        exists: bool,
    },
    Type {
        path: String,
        type_set: TypeSet,
    },
    ElemMatch {
        path: String,
        spec: Document,
        value_case: bool,
    },
    Regex {
        path: String,
        pattern: String,
        options: String,
    },
    Size {
        path: String,
        size: usize,
    },
    BitTest {
        path: String,
        mode: BitTestMode,
        positions: Vec<u32>,
    },
    Mod {
        path: String,
        divisor: i64,
        remainder: i64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateSpec {
    Replacement(Document),
    Modifiers(Vec<UpdateModifier>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateModifier {
    Set(String, Bson),
    Unset(String),
    Inc(String, Bson),
}
