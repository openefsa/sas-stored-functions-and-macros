# Data enrichment and validation (DEAV)

## Input and Output
The enrichment macro requires as input the following information:

1. __InputTable:__ A reference to a table which contains the input data which will be enriched by the procedure. Note that this table will be used as output for the enrichment, that is, the input table will be updated by adding the additional columns provided by the enrichment.

* __ErrorTable:__ A reference to a table which will contain the list of errors which occurred during the enrichment step.

* __Action:__ A conventional name specifying which enrichment/validation procedure is requested.
* __[Additional parameters depending on the requested actions]:__ Every enrichment/validation step has its own set of specific parameters. It is necessary to specify them, in order to make the enrichment and validation procedure work properly (see the next Section for further details).

At the end of the process, the required additional columns will be added to the input table. If errors/warnings were detected (either by the enrichment step or by the validation), then they will be inserted into the table specified in the second input parameter.


Note that it macro is only intended to be used as interface for the specific implementation of the DEAV steps, which need to be stored in separated procedures. This means that every enrichment/validation procedure has to be compiled/stored before calling it with this interface, otherwise an error will occur.


## Actions and related specific parameters
The current interface allows performing the following DEAV:

### FoodEx2 Validation

**Action code**: "FOODEX2_VALIDATION"

**Description**: It checks a generic FoodEx2 code correctness. Note that this does not enrich the input table, it performs a validation step only. The errors and warnings will be returned in the specified *ErrorTable* (input parameter).

**Required additional parameters**:

* **fx2v_foodex2Column**: (MANDATORY) FoodEx2 column of the input table which needs to be validated

* **fx2v_foodex2Hierarchy**: (OPTIONAL) code of FoodEx2 hierarchy which is used to extend the FoodEx2 checks. If set, the validation procedure will perform additional checks based on the selected hierarchy (e.g. checking non reportable terms)

### FoodEx2 to Matrix - Mapping
**Action code**: "FOODEX2_TO_MATRIX"

**Description**: It maps a FoodEx2 code to a matrix code, using the matrix tool and business logic. This is an enrichment step, which will create a new column containing the mapped matrix code.

**Required additional parameters**:

* **fx2TM_foodex2Column**: (MANDATORY) FoodEx2 column of the input table which will be mapped to the matrix codes

* **fx2TM_matrixColumn**: (MANDATORY) The name of the column which will be generated, containing the mapped matrix codes