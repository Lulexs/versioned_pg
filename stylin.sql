select id, price
from product
where price @= ('2025-07-10 22:12:49.06025+02', 150);
