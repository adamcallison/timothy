class PipelineError(Exception): ...


class MissingPipelineObjectError(PipelineError): ...


class MissingPipelineStageError(PipelineError): ...


class CannotSaveObjectError(PipelineError): ...


class DuplicateObjectError(PipelineError): ...


class DuplicateStageError(PipelineError): ...


class CannotCallStageError(PipelineError): ...


class InvalidResultsError(PipelineError): ...


class InvalidParamsError(PipelineError): ...


class CannotRunPipelineError(PipelineError): ...
