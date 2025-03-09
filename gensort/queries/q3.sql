--generate the distribution data

SELECT substr(k,1,1) as first_char, COUNT(*) as count 
FROM records 
GROUP BY substr(k,1,1) 
ORDER BY count DESC;
