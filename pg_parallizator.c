#include "postgres.h"
#include "libpq-fe.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "libpq/libpq-be.h"
#include "nodes/parsenodes.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/guc.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

typedef struct
{
	PGconn* con;
	char*   query;
} Work;

static ProcessUtility_hook_type PreviousProcessUtilityHook;
static int max_workers;
static int n_workers;
static Work* workers_queue;
static int queue_head;
static int queue_tail;

void _PG_init(void);

static void
WaitWorker(void)
{
	PGconn   *con = workers_queue[queue_tail].con;
	PGresult *res = PQgetResult(con);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		char	   *diag_sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
		char	   *message_primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
		char	   *message_detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
		char	   *message_hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
		char	   *message_context = PQresultErrorField(res, PG_DIAG_CONTEXT);
		int			sqlstate;

		if (diag_sqlstate)
			sqlstate = MAKE_SQLSTATE(diag_sqlstate[0],
									 diag_sqlstate[1],
									 diag_sqlstate[2],
									 diag_sqlstate[3],
									 diag_sqlstate[4]);
		else
			sqlstate = ERRCODE_CONNECTION_FAILURE;

		ereport(ERROR,
				(errcode(sqlstate),
				 message_primary ? errmsg_internal("%s", message_primary) :
				 errmsg("could not obtain message string for remote error"),
				 message_detail ? errdetail_internal("%s", message_detail) : 0,
				 message_hint ? errhint("%s", message_hint) : 0,
				 message_context ? errcontext("%s", message_context) : 0,
				 workers_queue[queue_tail].query));
	}
	free(workers_queue[queue_tail].query);
	PQclear(res);
	PQfinish(con);
	queue_tail = (queue_tail + 1) % max_workers;
	n_workers -= 1;
}

static void
WaitAllWorkers(int code, Datum arg)
{
	elog(LOG, "Wait %d workers", n_workers);
	while (n_workers != 0)
		WaitWorker();
}

static void
StartWorker(PGconn* con, char const* query)
{
	if (workers_queue == NULL)
		workers_queue = (Work*)malloc(sizeof(Work)*max_workers);
	elog(LOG, "Run query %s in parallel", query);

	workers_queue[queue_head].con = con;
	workers_queue[queue_head].query = strdup(query);
	queue_head = (queue_head + 1) % max_workers;
	n_workers += 1;
}

static void
ParProcessUtility(PlannedStmt *pstmt,
				  const char *queryString,
				  ProcessUtilityContext context, ParamListInfo params,
				  QueryEnvironment *queryEnv, DestReceiver *dest,
				  char *completionTag)
{
	Node *parsetree = pstmt->utilityStmt;
	if (nodeTag(parsetree) == T_IndexStmt
		&& !((IndexStmt*)parsetree)->concurrent
		&& context == PROCESS_UTILITY_TOPLEVEL
		&& strcmp(application_name, "pg_parallizator") != 0 /* avoid recursion  in case of extension preload */
		&& max_workers > 0)
	{
		char postmaster_port[8];
		PGconn* con;
		char const* keywords[] = {"port","dbname","user","sslmode","application_name","options",NULL};
		char const* values[] = { postmaster_port, MyProcPort->database_name, MyProcPort->user_name, "disable", "pg_parallizator", MyProcPort->cmdline_options};
		pg_itoa(PostPortNumber, postmaster_port);

		if (n_workers == max_workers)
		{
			elog(LOG, "Wait worker"); 
			WaitWorker();
		}

		con = PQconnectdbParams(keywords, values, false);

		if (PQstatus(con) == CONNECTION_BAD)
		{
			char	   *msg = pchomp(PQerrorMessage(con));
			PQfinish(con);
			ereport(ERROR,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
					 errmsg("could not establish connection"),
					 errdetail_internal("%s", msg)));
		}
		if (PQsendQuery(con, queryString))
		{
			StartWorker(con, queryString);
			return;
		}
		elog(LOG, "Failed to execute concurrently query %s", queryString);
		PQfinish(con);
	}
	else if (nodeTag(parsetree) == T_AlterTableStmt)
	{
		AlterTableStmt* alter = (AlterTableStmt*)parsetree;
		ListCell* cell;
		foreach (cell, alter->cmds)
		{
			AlterTableCmd* cmd = (AlterTableCmd*)lfirst(cell);
			if (cmd->subtype == AT_ClusterOn)
			{
				WaitAllWorkers(0, 0);
				break;
			}
		}
	}
	if (PreviousProcessUtilityHook != NULL)
	{
		PreviousProcessUtilityHook(pstmt, queryString,
								   context, params, queryEnv,
								   dest, completionTag);
	}
	else
	{
		standard_ProcessUtility(pstmt, queryString,
								context, params, queryEnv,
								dest, completionTag);
	}
}

void _PG_init(void)
{
   DefineCustomIntVariable("pg_parallizator.max_workers",
						   "Maximal number of indexes created in parallel",
						   NULL,
						   &max_workers,
						   8, 0, 1024,
						   PGC_USERSET,
						   0,
						   NULL,
						   NULL,
						   NULL);
	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = ParProcessUtility;
	on_proc_exit(WaitAllWorkers, 0);
}
