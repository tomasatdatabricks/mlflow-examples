Changelog
=========

0.8.1 (2018-12-21)
------------------

MLflow 0.8.1 introduces several significant improvements:

- Improved UI responsiveness and load time, especially when displaying experiments containing hundreds to thousands of runs.
- Improved visualizations, including interactive scatter plots for MLflow run comparisons
- Expanded support for scoring Python models as Spark UDFs. For more information, see the `updated documentation for this feature <https://mlflow.org/docs/latest/models.html#export-a-python-function-model-as-an-apache-spark-udf>`_.
- By default, saved models will now include a Conda environment specifying all of the dependencies necessary for loading them in a new environment.

Features:

- [API/CLI] Support for running MLflow projects from ZIP files (#759, @jmorefieldexpe)
- [Python API] Support for passing model conda environments as dictionaries to ``save_model`` and ``log_model`` functions (#748, @dbczumar)
- [Models] Default Anaconda environments have been added to many Python model flavors. By default, models produced by `save_model` and `log_model` functions will include an environment that specifies all of the versioned dependencies necessary to load and serve the models. Previously, users had to specify these environments manually. (#705, #707, #708, #749, @dbczumar)
- [Scoring] Support for synchronous deployment of models to SageMaker (#717, @dbczumar)
- [Tracking] Include the Git repository URL as a tag when tracking an MLflow run within a Git repository (#741, @whiletruelearn, @mateiz)
- [UI] Improved runs UI performance by using a react-virtualized table to optimize row rendering (#765, #762, #745, @smurching)
- [UI] Significant performance improvements for rendering run metrics, tags, and parameter information (#764, #747, @smurching)
- [UI] Scatter plots, including run comparsion plots, are now interactive (#737, @mateiz)
- [UI] Extended CSRF support by allowing the MLflow UI server to specify a set of expected headers that clients should set when making AJAX requests (#733, @aarondav)

Bug fixes and documentation updates:

- [Python/Scoring] MLflow Python models that produce Pandas DataFrames can now be evaluated as Spark UDFs correctly. Spark UDF outputs containing multiple columns of primitive types are now supported (#719, @tomasatdatabricks)
- [Scoring] Fixed a serialization error that prevented models served with Azure ML from returning Pandas DataFrames (#754, @dbczumar)
- [Docs] New example demonstrating how the MLflow REST API can be used to create experiments and log run information (#750, kjahan)
- [Docs] R documentation has been updated for clarity and style consistency (#683, @stbof)
- [Docs] Added clarification about user setup requirements for executing remote MLflow runs on Databricks (#736, @andyk)

Small bug fixes and doc updates (#768, #715, @smurching; #728, dodysw; #730, mshr-h; #725, @kryptec; #769, #721, @dbczumar; #714, @stbof)


0.8.0 (2018-11-08)
-----------------

MLflow 0.8.0 introduces several major features:

- Dramatically improved UI for comparing experiment run results:

  - Metrics and parameters are by default grouped into a single column, to avoid an explosion of mostly-empty columns. Individual metrics and parameters can be moved into their own column to help compare across rows.
  - Runs that are "nested" inside other runs (e.g., as part of a hyperparameter search or multistep workflow) now show up grouped by their parent run, and can be expanded or collapsed altogether. Runs can be nested by calling ``mlflow.start_run`` or ``mlflow.run`` while already within a run.
  - Run names (as opposed to automatically generated run UUIDs) now show up instead of the run ID, making comparing runs in graphs easier.
  - The state of the run results table, including filters, sorting, and expanded rows, is persisted in browser local storage, making it easier to go back and forth between an individual run view and the table.

- Support for deploying models as Docker containers directly to Azure Machine Learning Service Workspace (as opposed to the previously-recommended solution of Azure ML Workbench).


Breaking changes:

- [CLI] ``mlflow sklearn serve`` has been removed in favor of ``mlflow pyfunc serve``, which takes the same arguments but works against any pyfunc model (#690, @dbczumar)


Features:

- [Scoring] pyfunc server and SageMaker now support the pandas "split" JSON format in addition to the "records" format. The split format allows the client to specify the order of columns, which is necessary for some model formats. We recommend switching client code over to use this new format (by sending the Content-Type header ``application/json; format=pandas-split``), as it will become the default JSON format in MLflow 0.9.0. (#690, @dbczumar)
- [UI] Add compact experiment view (#546, #620, #662, #665, @smurching)
- [UI] Add support for viewing & tracking nested runs in experiment view (#588, @andrewmchen; #618, #619, @aarondav)
- [UI] Persist experiments view filters and sorting in browser local storage (#687, @smurching)
- [UI] Show run name instead of run ID when present (#476, @smurching)
- [Scoring] Support for deploying Models directly to Azure Machine Learning Service Workspace (#631, @dbczumar)
- [Server/Python/Java] Add ``rename_experiment`` to Tracking API (#570, @aarondav)
- [Server] Add ``get_experiment_by_name`` to RestStore (#592, @dmarkhas)
- [Server] Allow passing gunicorn options when starting mlflow server (#626, @mparkhe)
- [Python] Cloudpickle support for sklearn serialization (#653, @dbczumar)
- [Artifacts] FTP artifactory store added (#287, @Shenggan)


Bug fixes and documentation updates:

- [Python] Update TensorFlow integration to match API provided by other flavors (#612, @dbczumar; #670, @mlaradji)
- [Python] Support for TensorFlow 1.12 (#692, @smurching)
- [R] Explicitly loading Keras module at predict time no longer required (#586, @kevinykuo)
- [R] pyfunc serve can correctly load models saved with the R Keras support (#634, @tomasatdatabricks)
- [R] Increase network timeout of calls to the RestStore from 1 second to 60 seconds (#704, @aarondav)
- [Server] Improve errors returned by RestStore (#582, @andrewmchen; #560, @smurching)
- [Server] Deleting the default experiment no longer causes it to be immediately recreated (#604, @andrewmchen; #641, @schipiga)
- [Server] Azure Blob Storage artifact repo supports Windows paths (#642, @marcusrehm)
- [Server] Improve behavior when environment and run files are corrupted (#632, #654, #661, @mparkhe)
- [UI] Improve error page when viewing nonexistent runs or views (#600, @andrewmchen; #560, @andrewmchen)
- [UI] UI no longer throws an error if all experiments are deleted (#605, @andrewmchen)
- [Docs] Include diagram of workflow for multistep example (#581, @dennyglee)
- [Docs] Add reference tags and R and Java APIs to tracking documentation (#514, @stbof)
- [Docs/R] Use CRAN installation (#686, @javierluraschi)

Small bug fixes and doc updates (#576, #594, @javierluraschi; #585, @kevinykuo; #593, #601, #611, #650, #669, #671, #679, @dbczumar; #607, @suzil; #583, #615, @andrewmchen; #622, #681, @aarondav; #625, @pogil; #589, @tomasatdatabricks; #529, #635, #684, @stbof; #657, @mvsusp; #682, @mateiz; #678, vfdev-5; #596, @yutannihilation; #663, @smurching)


0.7.0 (2018-10-01)
-----------------

MLflow 0.7.0 introduces several major features:

- An R client API (to be released on CRAN soon)
- Support for deleting runs (API + UI)
- UI support for adding notes to a run

The release also includes bugfixes and improvements across the Python and Java clients, tracking UI,
and documentation.

Breaking changes:

- [Python] The per-flavor implementation of load_pyfunc has been made private (#539, @tomasatdatabricks)
- [REST API, Java] logMetric now accepts a double metric value instead of a float (#566, @aarondav)

Features:

- [R] Support for R (#370, #471, @javierluraschi; #548 @kevinykuo)
- [UI] Add support for adding notes to Runs (#396, @aadamson)
- [Python] Python API, REST API, and UI support for deleting Runs (#418, #473, #526, #579 @andrewmchen)
- [Python] Set a tag containing the branch name when executing a branch of a Git project (#469, @adrian555)
- [Python] Add a set_experiment API to activate an experiment before starting runs (#462, @mparkhe)
- [Python] Add arguments for specifying a parent run to tracking & projects APIs (#547, @andrewmchen)
- [Java] Add Java set tag API (#495, @smurching)
- [Python] Support logging a conda environment with sklearn models (#489, @dbczumar)
- [Scoring] Support downloading MLflow scoring JAR from Maven during scoring container build (#507, @dbczumar)


Bug fixes:

- [Python] Print errors when the Databricks run fails to start (#412, @andrewmchen)
- [Python] Fix Spark ML PyFunc loader to work on Spark driver (#480, @tomasatdatabricks)
- [Python] Fix Spark ML load_pyfunc on distributed clusters (#490, @tomasatdatabricks)
- [Python] Fix error when downloading artifacts from a run's artifact root (#472, @dbczumar)
- [Python] Fix DBFS upload file-existence-checking logic during Databricks project execution (#510, @smurching)
- [Python] Support multi-line and unicode tags (#502, @mparkhe)
- [Python] Add missing DeleteExperiment, RestoreExperiment implementations in the Python REST API client (#551, @mparkhe)
- [Scoring] Convert Spark DataFrame schema to an MLeap schema prior to serialization (#540, @dbczumar)
- [UI] Fix bar chart always showing in metric view (#488, @smurching)


Small bug fixes and doc updates (#467 @drorata; #470, #497, #508, #518 @dbczumar;
#455, #466, #492, #504, #527 @aarondav; #481, #475, #484, #496, #515, #517, #498, #521, #522,
#573 @smurching; #477 @parkerzf; #494 @jainr; #501, #531, #532, #552 @mparkhe; #503, #520 @dmatrix;
#509, #532 @tomasatdatabricks; #484, #486 @stbof; #533, #534 @javierluraschi;
#542 @GCBallesteros; #511 @AdamBarnhard)


0.6.0 (2018-09-10)
------------------

MLflow 0.6.0 introduces several major features:

- A Java client API, available on Maven
- Support for saving and serving SparkML models as MLeap for low-latency serving
- Support for tagging runs with metadata, during and after the run completion
- Support for deleting (and restoring deleted) experiments

In addition to these features, there are a host of improvements and bugfixes to the REST API, Python API, tracking UI, and documentation. The `examples/ <https://github.com/mlflow/mlflow/tree/master/examples>`_ subdirectory has also been revamped to make it easier to jump in, and examples demonstrating multistep workflows and hyperparameter tuning have been added.

Breaking changes:

We fixed a few inconsistencies in the the ``mlflow.tracking`` API, as introduced in 0.5.0:

- ``MLflowService`` has been renamed ``MlflowClient`` (#461, @mparkhe)
- You get an ``MlflowClient`` by calling ``mlflow.tracking.MlflowClient()`` (previously, this was ``mlflow.tracking.get_service()``) (#461, @mparkhe)
- ``MlflowService.list_runs`` was changed to ``MlflowService.list_run_infos`` to reflect the information actually returned by the call. It now returns a ``RunInfo`` instead of a ``Run`` (#334, @aarondav)
- ``MlflowService.log_artifact`` and ``MlflowService.log_artifacts`` now take a ``run_id`` instead of ``artifact_uri``. This now matches ``list_artifacts`` and ``download_artifacts``  (#444, @aarondav)

Features:

- Java client API added with support for the MLflow Tracking API (analogous to ``mlflow.tracking``), allowing users to create and manage experiments, runs, and artifacts. The release includes a `usage example <https://github.com/mlflow/mlflow/blob/master/mlflow/java/client/src/main/java/org/mlflow/tracking/samples/QuickStartDriver.java>`_ and `Javadocs <https://mlflow.org/docs/latest/java_api/index.html>`_. The client is published to Maven under ``mlflow:mlflow`` (#380, #394, #398, #409, #410, #430, #452, @aarondav)
- SparkML models are now also saved in MLeap format (https://github.com/combust/mleap), when applicable. Model serving platforms can choose to serve using this format instead of the SparkML format to dramatically decrease prediction latency. SageMaker now does this by default (#324, #327, #331, #395, #428, #435, #438, @dbczumar)
- [API] Experiments can now be deleted and restored via REST API, Python Tracking API, and MLflow CLI (#340, #344, #367, @mparkhe)
- [API] Tags can now be set via a SetTag API, and they have been moved to ``RunData`` from ``RunInfo`` (#342, @aarondav)
- [API] Added ``list_artifacts`` and ``download_artifacts`` to ``MlflowService`` to interact with a run's artifactory (#350, @andrewmchen)
- [API] Added ``get_experiment_by_name`` to Python Tracking API, and equivalent to Java API (#373, @vfdev-5)
- [API/Python] Version is now exposed via ``mlflow.__version__``.
- [API/CLI] Added ``mlflow artifacts`` CLI to list, download, and upload to run artifact repositories (#391, @aarondav)
- [UI] Added icons to source names in MLflow Experiments UI (#381, @andrewmchen)
- [UI] Added support to view ``.log`` and ``.tsv`` files from MLflow artifacts UI (#393, @Shenggan; #433, @whiletruelearn)
- [UI] Run names can now be edited from within the MLflow UI (#382, @smurching)
- [Serving] Added ``--host`` option to ``mlflow serve`` to allow listening on non-local addressess (#401, @hamroune)
- [Serving/SageMaker] SageMaker serving takes an AWS region argument (#366, @dbczumar)
- [Python] Added environment variables to support providing HTTP auth (username, password, token) when talking to a remote MLflow tracking server (#402, @aarondav)
- [Python] Added support to override S3 endpoint for S3 artifactory (#451, @hamroune)
- MLflow nightly Python wheel and JAR snapshots are now available and linked from https://github.com/mlflow/mlflow (#352, @aarondav)

Bug fixes and documentation updates:

- [Python] ``mlflow run`` now logs default parameters, in addition to explicitly provided ones (#392, @mparkhe)
- [Python] ``log_artifact`` in FileStore now requires a relative path as the artifact path (#439, @mparkhe)
- [Python] Fixed string representation of Python entities, so they now display both their type and serialized fields (#371, @smurching)
- [UI] Entry point name is now shown in MLflow UI (#345, @aarondav)
- [Models] Keras model export now includes Tensorflow graph explicitly to ensure the model can always be loaded at deployment time (#440, @tomasatdatabricks)
- [Python] Fixed issue where FileStore ignored provided Run Name (#358, @adrian555)
- [Python] Fixed an issue where any ``mlflow run`` failing printed an extraneous exception (#365, @smurching)
- [Python] uuid dependency removed (#351, @antonpaquin)
- [Python] Fixed issues with remote execution on Databricks (#357, #361, @smurching; #383, #387, @aarondav)
- [Docs] Added `comprehensive example <https://github.com/mlflow/mlflow/tree/master/examples/multistep_workflow>`_ of doing a multistep workflow, chaining MLflow runs together and reusing results (#338, @aarondav)
- [Docs] Added `comprehensive example <https://github.com/mlflow/mlflow/tree/master/examples/hyperparam>`_ of doing hyperparameter tuning (#368, @tomasatdatabricks)
- [Docs] Added code examples to ``mlflow.keras`` API (#341, @dmatrix)
- [Docs] Significant improvements to Python API documentation (#454, @stbof)
- [Docs] Examples folder refactored to improve readability. The examples now reside in ``examples/`` instead of ``example/``, too (#399, @mparkhe)
- Small bug fixes and doc updates (#328, #363, @ToonKBC; #336, #411, @aarondav; #284, @smurching; #377, @mparkhe; #389, gioa; #408, @aadamson; #397, @vfdev-5; #420, @adrian555; #459, #463, @stbof)


0.5.2 (2018-08-24)
------------------

MLflow 0.5.2 is a patch release on top of 0.5.1 containing only bug fixes and no breaking changes or features.

Bug fixes:

- Fix a bug with ECR client creation that caused ``mlflow.sagemaker.deploy()`` to fail when searching for a deployment Docker image (#366, @dbczumar)


0.5.1 (2018-08-23)
------------------

MLflow 0.5.1 is a patch release on top of 0.5.0 containing only bug fixes and no breaking changes or features.

Bug fixes:

- Fix ``with mlflow.start_run() as run`` to actually set ``run`` to the created Run (previously, it was None) (#322, @tomasatdatabricks)
- Fixes to DBFS artifactory to throw an exception if logging an artifact fails (#309) and to mimic FileStore's behavior of logging subdirectories (#347, @andrewmchen)
- Fix for Python 3.7 support with tarfiles (#329, @tomasatdatabricks)
- Fix spark.load_model not to delete the DFS tempdir (#335, @aarondav)
- MLflow UI now appropriately shows entrypoint if it's not main (#345, @aarondav)
- Make Python API forward-compatible with newer server versions of protos (#348, @aarondav)
- Improved API docs (#305, #284, @smurching)


0.5.0 (2018-08-17)
------------------

MLflow 0.5.0 offers some major improvements, including Keras and PyTorch first-class support as models, SFTP support as an artifactory, a new scatterplot visualization to compare runs, and a more complete Python SDK for experiment and run management.

Breaking changes:

- The Tracking API has been split into two pieces, a "basic logging" API and a "tracking service" API. The "basic logging" API deals with logging metrics, parameters, and artifacts to the currently-active active run, and is accessible in ``mlflow`` (e.g., ``mlflow.log_param``). The tracking service API allow managing experiments and runs (especially historical runs) and is available in ``mlflow.tracking``. The tracking service API will look analogous to the upcoming R and Java Tracking Service SDKs. Please be aware of the following breaking changes:

  - ``mlflow.tracking`` no longer exposes the basic logging API, only ``mlflow``. So, code that was written like ``from mlflow.tracking import log_param`` will have to be ``from mlflow import log_param`` (note that almost all examples were already doing this).
  - Access to the service API goes through the ``mlflow.tracking.get_service()`` function, which relies on the same tracking server set by either the environment variable ``MLFLOW_TRACKING_URI`` or by code with ``mlflow.tracking.set_tracking_uri()``. So code that used to look like ``mlflow.tracking.get_run()`` will now have to do ``mlflow.tracking.get_service().get_run()``. This does not apply to the basic logging API.
  - ``mlflow.ActiveRun`` has been converted into a lightweight wrapper around ``mlflow.entities.Run`` to enable the Python ``with`` syntax. This means that there are no longer any special methods on the object returned when calling ``mlflow.start_run()``. These can be converted to the service API.

  - The Python entities returned by the tracking service API are now accessible in ``mlflow.entities`` directly. Where previously you may have used ``mlflow.entities.experiment.Experiment``, you would now just use ``mlflow.entities.Experiment``. The previous version still exists, but is deprecated and may be hidden in a future version.
- REST API endpoint `/ajax-api/2.0/preview/mlflow/artifacts/get` has been moved to `$static_prefix/get-artifact`. This change is coversioned in the JavaScript, so should not be noticeable unless you were calling the REST API directly (#293, @andremchen)

Features:

- [Models] Keras integration: we now support logging Keras models directly in the log_model API, model format, and serving APIs (#280, @ToonKBC)
- [Models] PyTorch integration: we now support logging PyTorch models directly in the log_model API, model format, and serving APIs (#264, @vfdev-5)
- [UI] Scatterplot added to "Compare Runs" view to help compare runs using any two metrics as the axes (#268, @ToonKBC)
- [Artifacts] SFTP artifactory store added (#260, @ToonKBC)
- [Sagemaker] Users can specify a custom VPC when deploying SageMaker models (#304, @dbczumar)
- Pyfunc serialization now includes the Python version, and warns if the major version differs (can be suppressed by using ``load_pyfunc(suppress_warnings=True)``) (#230, @dbczumar)
- Pyfunc serve/predict will activate conda environment stored in MLModel. This can be disabled by adding ``--no-conda`` to ``mlflow pyfunc serve`` or ``mlflow pyfunc predict`` (#225, @0wu)
- Python SDK formalized in ``mlflow.tracking``. This includes adding SDK methods for ``get_run``, ``list_experiments``, ``get_experiment``, and ``set_terminated``. (#299, @aarondav)
- ``mlflow run`` can now be run against projects with no ``conda.yaml`` specified. By default, an empty conda environment will be created -- previously, it would just fail. You can still pass ``--no-conda`` to avoid entering a conda environment altogether (#218, @smurching)

Bug fixes:

- Fix numpy array serialization for int64 and other related types, allowing pyfunc to return such results (#240, @arinto)
- Fix DBFS artifactory calling ``log_artifacts`` with binary data (#295, @aarondav)
- Fix Run Command shown in UI to reproduce a run when the original run is targeted at a subdirectory of a Git repo (#294, @adrian555)
- Filter out ubiquitious dtype/ufunc warning messages (#317, @aarondav)
- Minor bug fixes and documentation updates (#261, @stbof; #279, @dmatrix; #313, @rbang1, #320, @yassineAlouini; #321, @tomasatdatabricks; #266, #282, #289, @smurching; #267, #265, @aarondav; #256, #290, @ToonKBC; #273, #263, @mateiz; #272, #319, @adrian555; #277, @aadamson; #283, #296, @andrewmchen)


0.4.2 (2018-08-07)
------------------

Breaking changes: None

Features:

- MLflow experiments REST API and ``mlflow experiments create`` now support providing ``--artifact-location`` (#232, @aarondav)
- [UI] Runs can now be sorted by columns, and added a Select All button (#227, @ToonKBC)
- Databricks File System (DBFS) artifactory support added (#226, @andrewmchen)
- databricks-cli version upgraded to >= 0.8.0 to support new DatabricksConfigProvider interface (#257, @aarondav)

Bug fixes:

- MLflow client sends REST API calls using snake_case instead of camelCase field names (#232, @aarondav)
- Minor bug fixes (#243, #242, @aarondav; #251, @javierluraschi; #245, @smurching; #252, @mateiz)


0.4.1 (2018-08-03)
------------------

Breaking changes: None

Features:

- [Projects] MLflow will use the conda installation directory given by the $MLFLOW_CONDA_HOME
  if specified (e.g. running conda commands by invoking "$MLFLOW_CONDA_HOME/bin/conda"), defaulting
  to running "conda" otherwise. (#231, @smurching)
- [UI] Show GitHub links in the UI for projects run from http(s):// GitHub URLs (#235, @smurching)

Bug fixes:

- Fix GCSArtifactRepository issue when calling list_artifacts on a path containing nested directories (#233, @jakeret)
- Fix Spark model support when saving/loading models to/from distributed filesystems (#180, @tomasatdatabricks)
- Add missing mlflow.version import to sagemaker module (#229, @dbczumar)
- Validate metric, parameter and run IDs in file store and Python client (#224, @mateiz)
- Validate that the tracking URI is a remote URI for Databricks project runs (#234, @smurching)
- Fix bug where we'd fetch git projects at SSH URIs into a local directory with the same name as
  the URI, instead of into a temporary directory (#236, @smurching)


0.4.0 (2018-08-01)
------------------

Breaking changes:

- [Projects] Removed the ``use_temp_cwd`` argument to ``mlflow.projects.run()``
  (``--new-dir`` flag in the ``mlflow run`` CLI). Runs of local projects now use the local project
  directory as their working directory. Git projects are still fetched into temporary directories
  (#215, @smurching)
- [Tracking] GCS artifact storage is now a pluggable dependency (no longer installed by default). 
  To enable GCS support, install ``google-cloud-storage`` on both the client and tracking server via pip.
  (#202, @smurching)
- [Tracking] Clients running MLflow 0.4.0 and above require a server running MLflow 0.4.0
  or above, due to a fix that ensures clients no longer double-serialize JSON into strings when
  sending data to the server (#200, @aarondav). However, the MLflow 0.4.0 server remains
  backwards-compatible with older clients (#216, @aarondav)


Features:

- [Examples] Add a more advanced tracking example: using MLflow with PyTorch and TensorBoard (#203)
- [Models] H2O model support (#170, @ToonKBC)
- [Projects] Support for running projects in subdirectories of Git repos (#153, @juntai-zheng)
- [SageMaker] Support for specifying a compute specification when deploying to SageMaker (#185, @dbczumar)
- [Server] Added --static-prefix option to serve UI from a specified prefix to MLflow UI and server (#116, @andrewmchen)
- [Tracking] Azure blob storage support for artifacts (#206, @mateiz)
- [Tracking] Add support for Databricks-backed RestStore (#200, @aarondav)
- [UI] Enable productionizing frontend by adding CSRF support (#199, @aarondav)
- [UI] Update metric and parameter filters to let users control column order (#186, @mateiz)

Bug fixes:

- Fixed incompatible file structure returned by GCSArtifactRepository (#173, @jakeret)
- Fixed metric values going out of order on x axis (#204, @mateiz)
- Fixed occasional hanging behavior when using the projects.run API (#193, @smurching)

- Miscellaneous bug and documentation fixes from @aarondav, @andrewmchen, @arinto, @jakeret, @mateiz, @smurching, @stbof


0.3.0 (2018-07-18)
------------------

Breaking changes:

- [MLflow Server] Renamed ``--artifact-root`` parameter to ``--default-artifact-root`` in ``mlflow server`` to better reflect its purpose (#165, @aarondav)

Features:

- Spark MLlib integration: we now support logging SparkML Models directly in the log_model API, model format, and serving APIs (#72, @tomasatdatabricks)
- Google Cloud Storage is now supported as an artifact storage root (#152, @bnekolny)
- Support asychronous/parallel execution of MLflow runs (#82, @smurching)
- [SageMaker] Support for deleting, updating applications deployed via SageMaker (#145, @dbczumar)
- [SageMaker] Pushing the MLflow SageMaker container now includes the MLflow version that it was published with (#124, @sueann)
- [SageMaker] Simplify parameters to SageMaker deploy by providing sane defaults (#126, @sueann)
- [UI] One-element metrics are now displayed as a bar char (#118, @cryptexis)

Bug fixes:

- Require gitpython>=2.1.0 (#98, @aarondav)
- Fixed TensorFlow model loading so that columns match the output names of the exported model (#94, @smurching)
- Fix SparkUDF when number of columns >= 10 (#97, @aarondav)
- Miscellaneous bug and documentation fixes from @emres, @dmatrix, @stbof, @gsganden, @dennyglee, @anabranch, @mikehuston, @andrewmchen, @juntai-zheng

0.2.1 (2018-06-28)
------------------

This is a patch release fixing some smaller issues after the 0.2.0 release.

- Switch protobuf implementation to C, fixing a bug related to tensorflow/mlflow import ordering (issues #33 and #77, PR #74, @andrewmchen)
- Enable running mlflow server without git binary installed (#90, @aarondav)
- Fix Spark UDF support when running on multi-node clusters (#92, @aarondav)

0.2.0 (2018-06-27)
------------------

- Added ``mlflow server`` to provide a remote tracking server. This is akin to ``mlflow ui`` with new options:

  - ``--host`` to allow binding to any ports (#27, @mdagost)
  - ``--artifact-root`` to allow storing artifacts at a remote location, S3 only right now (#78, @mateiz)
  - Server now runs behind gunicorn to allow concurrent requests to be made (#61, @mateiz)

- Tensorflow integration: we now support logging Tensorflow Models directly in the log_model API, model format, and serving APIs (#28, @juntai-zheng)
- Added ``experiments.list_experiments`` as part of experiments API (#37, @mparkhe)
- Improved support for unicode strings (#79, @smurching)
- Diabetes progression example dataset and training code (#56, @dennyglee)
- Miscellaneous bug and documentation fixes from @Jeffwan, @yupbank, @ndjido, @xueyumusic, @manugarri, @tomasatdatabricks, @stbof, @andyk, @andrewmchen, @jakeret, @0wu, @aarondav

0.1.0 (2018-06-05)
------------------

- Initial version of mlflow.
