SELECT
  MEASUREMENT_CONCEPT_ID,
  MEASUREMENT_DATE,
	VALUE_AS_NUMBER
	FROM  @CDM_schema.measurement measer
WHERE person_id = @PERSON_ID