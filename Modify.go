package ChangeCoreLibrary

import (
	"bitbucket.org/tokom_/core_sdk"
	"strconv"
	"bitbucket.org/tokom_/linkcore"
	"database/sql"
	"github.com/Afternight/Catch"
	"errors"
	"fmt"
	"strings"
)

func ModifyChangeCore(request linkcore.ModifyRequest, db *sql.DB, origin string) (linkcore.ModifyResponse){
	//important consideration for this log is that modify is idempotent
	//This means that we are more likely to simply say send the request again if there was any kind of failure
	//this is also a necessity as there is no direct delta modifier action like install and link
	log := new(Catch.Log)

	//Create response
	response := request.ConstructNewResponseObject()
	resp := response.(linkcore.ModifyResponse)

	totalUpdateStatement := request.GetStatementArray()

	if len(totalUpdateStatement) == 0 {
		log.AddNewFailureFromError(400,core_sdk.ProductDomain,errors.New("No values given to modify"),true,request.GetModifyRectifier(origin))
		resp.SetLog(*log)
		return resp
	}

	query := fmt.Sprintf("UPDATE OBJ_Products SET %s WHERE PK_OBJ = ?", strings.Join(totalUpdateStatement,","))
	//Exec the update
	_ , dbErr := db.Exec(query,request.GetPrimaryKey())

	//if the db failed knockout
	if dbErr != nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,dbErr,true,request.GetModifyRectifier(origin))
		resp.SetLog(*log)
		return resp
	}

	//if no ID's are given, obtain them to then push the modified changes
	if request.GetIDs() == nil {
		newIDs, idErr := core_sdk.GetObjectCorrelations(db,strconv.FormatInt(request.GetPrimaryKey(),10),request.GetObjectHandle())
		if idErr != nil {
			//if we fail to get the ID's, we treat it as a non fatal failure since we have the full object but we must return
			//immediately as the rest of hte function relies on those ID's
			log.AddNewFailureFromError(500,core_sdk.ProductDomain,idErr,false,request.GetModifyRectifier(origin))
			resp.SetLog(*log)
			resp.SetObjectFromPK(db,request.GetPrimaryKey()) //note this function has the side effect of directly logging object errors
			return resp
		}
		request.SetIDs(newIDs)
	}

	//Get Switchboard ID's
	var stubbedSwitch []core_sdk.IntegrationCode //todo make this actually get from switchboard

	//Combine the two ID sets together
	request.SetIDs(CreateIDsFromSources(request.GetIDs(),stubbedSwitch))

	//Enact the changes externally
	//We don't care for codes because we assume that no errors means full execution
	//We don't care for status because any status errors are logged in the deltaLog along with their rectifier
	deltaReq, _, deltaErr := request.EnactDelta()

	if deltaErr != nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,deltaErr,false, request.GetModifyRectifier(origin))
		resp.SetLog(*log)
		resp.SetObjectFromPK(db,request.GetPrimaryKey())
		return resp
	}

	deltaComReq := deltaReq.(linkcore.DeltaResponse)

	//merge logs capturing any failures
	log.MergeLogs(deltaComReq.GetLog())

	//check if we failed to execute the delta, if we did we can bypass the full rerun by passing all the info
	//we already know
	if deltaErr != nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,deltaErr,false, request.GetModifyRectifier(core_sdk.DeltaDomain))
	}


	resp.SetLog(*log)
	resp.SetObjectFromPK(db,request.GetPrimaryKey())
	return resp
}
