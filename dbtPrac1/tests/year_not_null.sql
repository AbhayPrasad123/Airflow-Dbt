select *
from {{ ref('ipl_csv_json_join') }}
where year is null