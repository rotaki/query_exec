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
  * [?] Q16 (Partially done. Without markjoin, n^3 complexity)
  * [x] Q17
  * [?] Q18 (Partially done. Without markjoin, n^3 complexity)
  * [x] Q19 (Expanding OR causes super complicated filter predicates)
  * [?] Q20 (Partially done. Without markjoin, n^3 complexity)
  * [ ] Q21 (Semi-join for uncorrelated exists)
  * [ ] Q22 (Substring)
* [] Remove unnecessary projections in the query plan