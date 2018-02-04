SELECT
	PROCEDURE_CONCEPT_ID,
	PROCEDURE_DATE
	FROM @CDM_schema.procedure_occurrence procc
  WHERE procc.person_id = @PERSON_ID
