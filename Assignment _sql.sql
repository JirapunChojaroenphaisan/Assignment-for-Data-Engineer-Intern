--โจทย์ข้อ 1
SELECT di.dir_id,
       di.dir_fname,
       di.dir_lname,
       ge.gen_title,
       MAX(ra.num_o_ratings) AS max_ratings
FROM director  di
INNER JOIN movie_direction  md ON di.dir_id  = md.dir_id
INNER JOIN movie mo ON md.mov_id= mo.mov_id
INNER JOIN movie_genres mo_g ON mo.mov_id = mo_g.mov_id
INNER JOIN  genres ge ON mo_g.gen_id = ge.gen_id
INNER JOIN rating ra ON mo.mov_id = ra.mov_id
GROUP BY  ge.gen_title,di.dir_id
ORDER BY  ge.gen_title
;
--โจทย์ข้อ 2
SELECT ac.*,
       SUM(mo.mov_time) AS sum_time
             
FROM actor ac 
JOIN movie_cast mc ON ac.act_id = mc.act_id
JOIN movie mo ON mc.mov_id = mo.mov_id  
JOIN rating ra ON mo.mov_id = ra.mov_id   
WHERE (ra.num_o_ratings IS NOT NULL)
GROUP BY ac.act_id
;
--โจทย์ข้อ 3
SELECT   di.*,
         ac.*,
         COUNT(ac.act_id) AS work_time
              
FROM actor ac 
JOIN movie_cast mc ON ac.act_id = mc.act_id
JOIN movie mo ON mc.mov_id = mo.mov_id
JOIN movie_direction md ON mo.mov_id = md.mov_id
JOIN director di ON md.dir_id = di.dir_id
WHERE act_gender = 'f' 
AND ac.act_id IN (SELECT ac.act_id 
FROM actor 
WHERE act_gender = 'f'
GROUP BY id 
ORDER BY COUNT(*) DESC LIMIT 5)

GROUP BY di.dir_id,ac.act_id
ORDER BY di.dir_id,work_time
;
--โจทย์ข้อ 4
ALTER TABLE genres
ADD gen_int INT NOT NULL;

UPDATE genres
 SET gen_int =  (SELECT ROW_NUMBER() OVER(ORDER BY gen_title )
 FROM genres )

ALTER TABLE genres
DROP COLUMN gen_title;

--โจทย์ข้อ 5
UPDATE actor
SET act_gender = 'f'
WHERE (LOWER(act_fname) LIKE 'em%' OR LOWER(act_fname) LIKE 'char%') 
OR (LOWER(act_fname) LIKE '%lia' OR LOWER(act_fname) LIKE '%sy' OR LOWER(act_fname) LIKE '%dy')
;