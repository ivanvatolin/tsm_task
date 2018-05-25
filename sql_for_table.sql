select id, max(value) as value, max(date) as date from t
group by id