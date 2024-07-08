use std::{cell::RefCell, collections::HashSet, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HeuristicRule {
    Hoist,
    Decorrelate,
    SelectionPushdown,
    ProjectionPushdown,
}

pub struct HeuristicRules {
    rules: RefCell<HashSet<HeuristicRule>>,
}

impl HeuristicRules {
    pub fn new() -> HeuristicRules {
        HeuristicRules {
            rules: RefCell::new(HashSet::new()),
        }
    }

    pub fn enable(&self, rule: HeuristicRule) {
        self.rules.borrow_mut().insert(rule);
    }

    pub fn disable(&self, rule: HeuristicRule) {
        self.rules.borrow_mut().remove(&rule);
    }

    pub fn is_enabled(&self, rule: &HeuristicRule) -> bool {
        self.rules.borrow().contains(rule)
    }
}

impl Default for HeuristicRules {
    fn default() -> Self {
        let mut rules = HashSet::new();
        rules.insert(HeuristicRule::Hoist);
        rules.insert(HeuristicRule::Decorrelate);
        rules.insert(HeuristicRule::SelectionPushdown);
        rules.insert(HeuristicRule::ProjectionPushdown);
        HeuristicRules {
            rules: RefCell::new(rules),
        }
    }
}

pub type HeuristicRulesRef = Arc<HeuristicRules>;
