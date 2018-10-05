DEFINE SQRT org.apache.pig.piggybank.evaluation.math.SQRT;
DEFINE POW org.apache.pig.piggybank.evaluation.math.POW;

points = load '$input' USING PigStorage(',') AS(id:chararray, x1:double, y1:double);

distances = FOREACH points GENERATE id,SQRT(POW((x1-$x),2) + POW((y1-$y),2)) AS distance;

sorted = ORDER distances BY distance ASC;

topK = LIMIT sorted $k;

STORE topK into '$output';





