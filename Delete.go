package ChangeCoreLibrary

import (
	"bitbucket.org/tokom_/linkcore"
	"database/sql"
	"github.com/Afternight/Catch"
	"bitbucket.org/tokom_/core_sdk"
	"strconv"
	"fmt"
	"strings"
)

func DeleteChangeCore(request linkcore.DeleteRequest, db *sql.DB, origin string) (linkcore.DeleteResponse) {
	response := request.ConstructNewResponseObject()
	resp := response.(linkcore.DeleteResponse)
	log := new(Catch.Log)

	//if no ID's are given, obtain all of them from the database
	if request.GetIDs() == nil {
		newIDs, idErr := core_sdk.GetObjectCorrelations(db,strconv.FormatInt(request.GetPrimaryKey(),10),request.GetObjectHandle())
		if idErr != nil {
			//we can't delete from our db if there are any correlations existing
			//therefore if we can't get those correlations we knockout
			log.AddNewFailureFromError(500,core_sdk.ProductDomain,idErr,true,request.GetDeleteRectifier(origin))
			resp.SetLog(*log)
			resp.SetPayload(false)
			return resp
		}
		request.SetIDs(newIDs)
	}

	//Get Switchboard ID's
	var stubbedSwitch []core_sdk.IntegrationCode //todo make this actually get from switchboard

	//Combine the two ID sets together
	request.SetIDs(CreateIDsFromSources(request.GetIDs(),stubbedSwitch))

	//push request to delta
	deltaReq, _, deltaErr := request.EnactDelta()

	if deltaErr != nil || deltaReq == nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,deltaErr,true, request.GetDeleteRectifier(origin))
		resp.SetLog(*log)
		resp.SetPayload(false)
		return resp
	}

	deltaResp := deltaReq.(linkcore.DeltaResponse)
	log.MergeLogs(deltaResp.GetLog())

	if deltaResp.GetLog().Fatality {
		resp.SetLog(*log)
		resp.SetPayload(false)
		return resp
	}

	var deleteStatements []string

	if len(deltaResp.GetIDs()) > 0 {
		for _, val := range deltaResp.GetIDs() {
			stringVal := fmt.Sprintf("(extCode = '%s' and extID = '%s' and FK_OBJ = %d)",val.IntegrationCode,val.IntegrationID,request.GetPrimaryKey())
			deleteStatements = append(deleteStatements,stringVal)
		}

		corrDBQuery := fmt.Sprintf("DELETE FROM COR_Products where %s",strings.Join(deleteStatements," or "))

		_, corrErr := db.Exec(corrDBQuery)

		if corrErr != nil {
			log.AddNewFailureFromError(500,core_sdk.ProductDomain,corrErr,true, request.GetDeleteRectifier(origin))
			resp.SetLog(*log)
			resp.SetPayload(false)
			return resp
		}
	}

	_, dbErrCass := db.Exec("DELETE FROM OBJ_Products WHERE PK_OBJ = ?",strconv.FormatInt(request.GetPrimaryKey(), 10))

	if dbErrCass != nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,deltaErr,false, request.GetDeleteRectifier(origin))
		resp.SetLog(*log)
		resp.SetPayload(false)
		return resp
	}
	resp.SetLog(*log)
	resp.SetPayload(true)
	return resp
}
