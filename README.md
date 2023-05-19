## Project overview:

The join is a fundamental operation in relational data processing that finds matching rows between two tables. In this project, you will implement, test, and benchmark a disk-based join algorithm. Your goal is to efficiently use memory and disk resources to return the answer to the following query.

```sql
SELECT R.b, S.b
FROM R, S
WHERE R.a = S.a;
```

## Main files:

join.cpp, file.hpp, file.cpp, and join.hpp

## Main coding language:

C++ 17

## Attributions:

I (Zelong Jiang) wrote join.cpp.

My instructor (Kevin Gaffney) wrote file.hpp, file.cpp, and join.hpp
