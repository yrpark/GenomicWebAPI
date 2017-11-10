select person.PERSON_ID,
	person.GENDER_SOURCE_VALUE,
	person.YEAR_OF_BIRTH,
	person.MONTH_OF_BIRTH,
  person.DAY_OF_BIRTH,
  death.DEATH_DATE
FROM  @CDM_schema.person
LEFT OUTER JOIN @CDM_schema.death ON  @CDM_schema.person.person_id =  @CDM_schema.death.person_id
WHERE person.person_id = @PERSON_ID
