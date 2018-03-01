package ChangeCoreLibrary

import (
	"bitbucket.org/tokom_/linkcore"
	"database/sql"
	"github.com/Afternight/Catch"
)

func InstallChangeCore (request linkcore.InstallRequest, db *sql.DB, origin string) (linkcore.InstallResponse) {
	log := new(Catch.Log)
	resp := request.ConstructNewResponseObject().(linkcore.InstallResponse)

	//note that the codes and such are created according to what was sent
	createRequest, createErr := request.ConstructCreateRequest(db)
	if createErr != nil {
		log.AddNewFailureFromError(500,origin,createErr,true,request.GetInstallRectifier(origin))
		resp.SetLog(*log)
		return resp
	}

	//consider adding switchboard check here to allow null passing of codes, currently install requires

	//create object in our integrations
	dO, _, deltaErr := createRequest.EnactDelta()

	//check if there was an error in sending the delta request
	if deltaErr != nil || dO == nil {
		log.AddNewFailureFromError(500,origin,deltaErr,true,request.GetInstallRectifier(origin))
		resp.SetLog(*log)
		return resp
	}

	deltaComReq := dO.(linkcore.DeltaResponse)
	//merge our logs, collecting our delta failures
	log.MergeLogs(deltaComReq.GetLog())

	if deltaComReq.GetLog().Fatality { //check if there was a fatality on delta
		resp.SetLog(*log)
		return resp
	}

	//if we are here we assume delta enacted atleast partially and begin installing
	corrErr := AddCorrelations(request.GetPrimaryKey(), deltaComReq.GetIDs(),db,request.GetObjectHandle())

	//A corr err in this case means a complete failure of correlation insertion
	//link in itself is idempotent, so sending a rectifier over the top is only a performance problem
	//todo add function that checks on resps set object if it has gotten all ID's that were hit with delta, if not run in the link rectifier
	if corrErr != nil {
		log.AddNewFailureFromError(500, origin,corrErr,false,request.GetLinkRectifier(origin,deltaComReq.GetIDs()))
	}

	resp.SetObjectFromPK(db,request.GetPrimaryKey())
	resp.SetLog(*log)
	return resp
}
