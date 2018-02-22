package ChangeCoreLibrary

import (
	"bitbucket.org/tokom_/core_sdk"
	"fmt"
	"database/sql"
	"strconv"
	"strings"
	"bitbucket.org/tokom_/linkcore"
	"github.com/Afternight/Catch"
	"bitbucket.org/tokom_/changecorelibrary"
	"errors"
)

func CreateCodesFromSources(codes []core_sdk.IntegrationCode, switchboard []core_sdk.IntegrationCode, inverted bool) core_sdk.IntCodes {
	completeCodes := core_sdk.IntCodes{}
	if len(codes) == 0 {
		completeCodes.Values = switchboard
	} else {
		completeCodes.Values = codes
	}
	completeCodes.Inverted = inverted
	return completeCodes
}

func CreateIDsFromSources(ids []core_sdk.ExtID, switchboard []core_sdk.IntegrationCode) ([]core_sdk.ExtID){
	if len(switchboard) == 0 {
		return ids
	} else {
		var NewIdSet []core_sdk.ExtID

		switchMap := make(map[string]bool)

		for _, val := range switchboard {
			switchMap[string(val)] = true
		}

		//return the overlap
		for _, val := range ids {
			if switchMap[string(val.IntegrationCode)] {
				NewIdSet = append(NewIdSet, val)
			}
		}
		return NewIdSet
	}
}

func AddCorrelations(pk int64, newIDs []core_sdk.ExtID, db *sql.DB, tableName string) error {
	var insertStatements []string
	for _, extID := range newIDs {
		stringVal := fmt.Sprintf("('%s','%s',%s)",extID.IntegrationCode,extID.IntegrationID,strconv.FormatInt(pk,10))
		insertStatements = append(insertStatements,stringVal)
	}

	if len(insertStatements) > 0 {
		corrDBQuery := fmt.Sprintf("insert into COR_%s (extCode,extID,FK_OBJ) values %s", tableName, strings.Join(insertStatements, ","))
		_, dbExecErr := db.Exec(corrDBQuery)
		if dbExecErr != nil {
			return dbExecErr
		}
	}

	return nil
}

func StandardCoreSwitch(ogReq linkcore.Request, db *sql.DB, origin string) (Catch.IsLogged){
	switch v := ogReq.(type) {
	case *linkcore.CreateRequest:
		return ChangeCoreLibrary.CreateChangeCore(v,db,origin)
	case *linkcore.ModifyRequest:
		return ChangeCoreLibrary.ModifyChangeCore(v,db,origin)
	case *linkcore.DeleteRequest:
		return ChangeCoreLibrary.DeleteChangeCore(v,db,origin)
	case *linkcore.InstallRequest:
		return ChangeCoreLibrary.InstallChangeCore(v,db,origin)
	case *linkcore.LinkRequest:
		return ChangeCoreLibrary.LinkChangeCore(v,db,origin)
	default:
		log := new(Catch.Log)
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,errors.New("Request Not Implemented"),true,Catch.Rectifier{}) //todo fix this pass
		return log
	}
	//todo push the change here to switchboard
}