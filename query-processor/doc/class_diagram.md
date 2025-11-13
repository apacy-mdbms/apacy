Kelompok Bash (Apacy - 5)

|NIM | Nama  |
|---|---|
|13523149|Naufarrel Zhafif Abhista|
|13523153|Muhammad Farrel Wibowo|
|13523160|I Made Wiweka Putera|
|13523152|Muhammad Kinan Arkansyaddad|
||M. Zulfiansyah Bayu Pratama|


```mermaid
classDiagram
direction BT
	namespace com.apacy.queryprocessor {
		class Main {
			+static main(String\[\] args)
		}
	
		class QueryProcessor {
			-IQueryOptimizer qo
			-IStorageManager sm
			-IConcurrencyControlManager ccm
			-IFailureRecoveryManager frm
			-PlanTranslator planTranslator
			-JoinStrategy joinStrategy
			-SortStrategy sortStrategy
			+executeQuery(String sqlQuery)
			+initialize()
			+shutdown()
		}
	
		class PlanTranslator {
			+translateToRetrieval(ParsedQuery, String)
			+translateToWrite(ParsedQuery, String, boolean)
			+translateToDeletion(ParsedQuery, String)
		}
	}
	

  namespace com.apacy.queryprocessor.execution {
    class JoinStrategy {
      +static nestedLoopJoin(List, List, String)
      +static hashJoin(List, List, String)
      +static sortMergeJoin(List, List, String)
    }

    class SortStrategy {
      +static sort(List, String, boolean)
      +static sortMultiple(List, String\[\], boolean\[\])
      +static externalSort(List, String, boolean, int)
    }
  }
  
  namespace com.apacy.queryprocessor.cli {
    class ApacyCLI {
      -QueryProcessor queryProcessor
      -Scanner scanner
      +start()
      -processInput(String)
      -executeQuery(String)
    }
  }

  namespace com.apacy.common {
    class DBMSComponent {
      +initialize()
      +shutdown()
    }
  }
  
	Main ..> QueryProcessor 
  Main ..> ApacyCLI 

  ApacyCLI o-- QueryProcessor 

  QueryProcessor --|> DBMSComponent 
  QueryProcessor o-- PlanTranslator 
  QueryProcessor o-- JoinStrategy 
  QueryProcessor o-- SortStrategy 


  JoinStrategy ..> SortStrategy
```