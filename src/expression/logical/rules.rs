use std::{cell::RefCell, collections::HashSet, rc::Rc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rule {
    Hoist,
    Decorrelate,
    SelectionPushdown,
    ProjectionPushdown,
}

pub struct Rules {
    rules: RefCell<HashSet<Rule>>,
}

impl Rules {
    pub fn new() -> Rules {
        Rules {
            rules: RefCell::new(HashSet::new()),
        }
    }

    pub fn enable(&self, rule: Rule) {
        self.rules.borrow_mut().insert(rule);
    }

    pub fn disable(&self, rule: Rule) {
        self.rules.borrow_mut().remove(&rule);
    }

    pub fn is_enabled(&self, rule: &Rule) -> bool {
        self.rules.borrow().contains(rule)
    }
}

impl Default for Rules {
    fn default() -> Self {
        let mut rules = HashSet::new();
        rules.insert(Rule::Hoist);
        rules.insert(Rule::Decorrelate);
        rules.insert(Rule::SelectionPushdown);
        rules.insert(Rule::ProjectionPushdown);
        Rules {
            rules: RefCell::new(rules),
        }
    }
}

pub type RulesRef = Rc<Rules>;
