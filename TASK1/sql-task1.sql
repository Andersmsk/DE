-- CREATE DATABASE dormitory;  -- creating database

CREATE TABLE IF NOT EXISTS room (										-- 'creating table  room + setting PRIMARY key'
				  id SERIAL NOT NULL PRIMARY KEY,
				  name VARCHAR (10) 
				  );
				 
CREATE TABLE IF NOT EXISTS student (									-- 'creating table student 
					  id SERIAL NOT NULL PRIMARY KEY,
					  birthday TIMESTAMP NOT NULL,
					  "name" VARCHAR (20) NOT NULL,
					  room INTEGER NOT NULL REFERENCES room(id),		-- 'FOREIGN KEY to the table room'
					  sex CHAR NOT NULL CHECK (sex IN ('F', 'M'))		-- ' Constraint CHECK that values must be F or M'
					  );
					 
SELECT * FROM public.room r ;
SELECT * FROM public.student s ;

/*
TRUNCATE room RESTART IDENTITY CASCADE;                 -- 'USE IN CASE IF YOU WANT CLEAR TABLE AND RESET IDENTITY VALUES (PR. KEY) WHILE TESTING python INSERTION'
TRUNCATE student RESTART IDENTITY CASCADE;  
*/

-- Список комнат и количество студентов в каждой из них

SELECT public.room.id AS room_id, 						-- 'selecting rows'
	   public.room."name" AS room_name , 
	   COUNT(public.student.id) AS students_quantity    -- 'count students BY ID'
FROM public.room
	INNER JOIN public.student							-- 'join table student BY room id'
	ON public.room.id = public.student.room 
GROUP BY public.room.id 								-- 'grouping rooms to Count stundets in each room'
ORDER BY public.room.id; 								-- 'ordering rooms DEFAULT ASCENDING'

-- 5 комнат, где самый маленький средний возраст студентов

SELECT public.room.id AS room_id, 												-- 'selecting rows'
	   public.room."name" AS room_name, 										
	   COUNT(public.student.id) AS students_quantity,							-- 'count students BY ID'
	   AVG(EXTRACT(YEAR FROM age(now(), public.student.birthday))) ::INTEGER AS average_age  -- 'extracting year from now and student birthday after Average it and set to Integer for better view'
FROM public.room
	INNER JOIN public.student		-- 'join table student BY room id'
	ON public.room.id = public.student.room 
GROUP BY public.room.id 
ORDER BY average_age ASC	-- 'ordering by avg_age ASC as default'
LIMIT 5;                    -- 'will show ONLY 5 rows'

-- 5 комнат с самой большой разницей в возрасте студентов

SELECT public.room.id AS room_id, 				-- 'selecting columns'
	   public.room."name" AS room_name, 
	   COUNT(public.student.id) AS students_quantity,
	   MAX(EXTRACT(YEAR FROM age(now(), public.student.birthday))) ::INTEGER -     -- 'find maximum student age and SUBSTRACT IT'
	   MIN(EXTRACT(YEAR FROM age(now(), public.student.birthday))) ::INTEGER AS stud_age_diff  -- 'FROM minimum student age'
FROM public.room
	INNER JOIN public.student         -- 'join table student by condition room.id'
	ON public.room.id = public.student.room 
GROUP BY public.room.id 
ORDER BY stud_age_diff DESC, students_quantity ASC    -- ' order by both conditions for having the same result everytime'
LIMIT 5;  -- 'limit only 5 rows'

-- Список комнат где живут разнополые студенты

SELECT public.room.id AS room_id,             -- 'selecting columns'
	   public.room."name" AS room_name , 
	   STRING_AGG(public.student.sex, ', ' ORDER BY public.student.sex) AS genders_in_room  -- 'here we use STING_AGG to put every sex of students in one row and ORDER it'
FROM public.room
	INNER JOIN public.student				--'joing table studen by condition ON room.id'
	ON public.room.id = public.student.room 
WHERE public.student.sex IN (UPPER('M'), UPPER('F'))   -- 'cheking for condition by the gender'
GROUP BY public.room.id 
HAVING COUNT(DISTINCT public.student.sex) = 2   -- 'here we check contition if two genders are in one room'
ORDER BY public.room.id;  

-- 'to speed up queries we can add indexes on columns which more used'

CREATE INDEX IF NOT EXISTS idx_room_name ON public.room("name");  -- 'creating index on room name in the table room'

CREATE INDEX IF NOT EXISTS idx_student_birtday ON public.student(birthday);  -- 'creating index on students' birthday in the table students'

CREATE INDEX IF NOT EXISTS idx_student_room ON public.student(room); -- 'creating index on room numbers in student table'
