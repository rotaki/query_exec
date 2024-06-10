# TODO

* [x] Orderby clause
* [] Limit clause
* [] Subquery in the from clause
* [] Fuzz testing
* [] Support all the TPC-H queries
  * [x] Q1
  * [x] Q2
  * [x] Q3
  * [x] Q4
  * [x] Q5
  * [x] Q6
  * [x] Q7
  * [x] Q8 (Cast)
  * [x] Q9
  * [x] Q10
  * [x] Q11 (Sum with subquery)
  * [x] Q12 (Inlist)
  * [x] Q13
  * [x] Q14
  * [ ] Q15 (Create view)
  * [x] Q16 (Mark join)
  * [x] Q17
  * [x] Q18 (Mark join)
  * [x] Q19 (Expanding OR causes super complicated filter predicates)
  * [x] Q20 (Mark join)
  * [x] Q21 (Semi-join for uncorrelated exists)
  * [ ] Q22 (Substring)
* [ ] Remove unnecessary projections in the query plan
* [ ] Convert mark join to semi-join or anti-join if possible
* [ ] Add complicated selection push-downs
  * For example, say we have join(t1@c1==t2@c2).filter(@c1>0). Then predicate (@c1>0) can not only be pushed down to t1, but also to t2 as (@c2>0)
