# Project workflow instructions

When a user asks to upload, publish, deploy, refresh, replace, or put this project in Databricks—or mentions making a wheel, a `.whl`, `%pip install --force-reinstall`, or the Databricks workspace package—read and follow:

`skills/databricks-wheel-publisher/SKILL.md`

Treat casual wording such as “upload this to Databricks,” “put the new wheel there,” “replace the old one,” and “make the notebook use my latest changes” as triggers for that workflow. Check OAuth authentication before any upload, build a fresh wheel, overwrite the exact configured workspace path, and verify the remote artifact afterward.
