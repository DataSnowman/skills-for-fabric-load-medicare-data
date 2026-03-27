updateDefinitionNotebookEndpoint.md

I have a question about how you attached the Lakehouse to both the of Notebooks.  When I tried Claude yesterday
  it was not able to attach the Lakehouse and had to improvise by creating a notebooks.py file

● Great question! The key was embedding the lakehouse binding inside the notebook definition itself rather than
  trying to PATCH it after creation.

  What worked here

  When I updated the notebooks via the updateDefinition endpoint, I added the lakehouse connection into the
  notebook's metadata.dependencies section:

   "dependencies": {
     "lakehouse": {
       "default_lakehouse": "<LH_ID>",
       "default_lakehouse_name": "MedicareSkillsF4TerminalLHghcpcli",
       "default_lakehouse_workspace_id": "<WS_ID>",
       "known_lakehouses": [{ "id": "<LH_ID>" }]
     }
   }

  Then deployed it via:

   POST /v1/workspaces/{WS_ID}/notebooks/{NB_ID}/updateDefinition

  This is the same structure Fabric uses internally when you attach a lakehouse in the UI — it's stored in the
  .ipynb metadata.

  What didn't work

   1. PATCH /notebooks/{id} with a defaultLakehouse body — the API rejected it as "no valid field to update"
   2. Including a "type": "Notebook" field in the definition body — the API returned "invalid input parameter:
  Type"

  Why Claude Code likely struggled

  The PATCH-based binding approach (documented in some older examples) doesn't work for all API versions. Claude
  Code probably tried that, got the same error, and fell back to a Python script approach. The trick is knowing
  that the lakehouse binding lives in the notebook metadata, not as a separate API call.