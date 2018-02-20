package ChangeCoreLibrary

import (
	"bitbucket.org/tokom_/linkcore"
	"database/sql"
	"github.com/Afternight/Catch"
)

func LinkChangeCore(request linkcore.LinkRequest, db *sql.DB, origin string) (linkcore.LinkResponse){
	log := new(Catch.Log)
	resp := request.ConstructNewResponseObject().(linkcore.LinkResponse)
	corrErr := AddCorrelations(request.GetPrimaryKey(),request.GetIDs(),db,request.GetObjectHandle())

	if corrErr != nil {
		log.AddNewFailureFromError(500,origin,corrErr,true,request.GetLinkRectifier(origin))
		resp.SetLog(*log)
		return resp
	}

	setErr := resp.SetObjectFromPK(db,request.GetPrimaryKey())

	if setErr != nil {
		log.AddNewFailureFromError(500,origin,corrErr,false,request.GetLinkRectifier(origin))
	}
	return resp
}
