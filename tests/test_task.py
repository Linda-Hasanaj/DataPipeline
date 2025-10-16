# tests/test_task.py
import builtins
from pipeline.task import Task

def test_task_initializes_with_name_and_empty_config():
    t = Task(name="Base")
    assert t.name == "Base"
    assert isinstance(t.config, dict)
    assert t.config == {}

def test_task_default_config_is_not_shared_between_instances():
    t1 = Task(name="T1")
    t2 = Task(name="T2")
    t1.config["a"] = 1
    assert t2.config == {}

def test_task_uses_provided_config_object():
    cfg = {"x": 42}
    t = Task(name="Custom", config=cfg)
    assert t.config is cfg
    assert t.config["x"] == 42

def test_log_prints_prefix_and_message(capsys):
    t = Task(name="Logger")
    t.log("hello")
    out = capsys.readouterr().out
    assert out == "[Logger] hello\n"

def test_log_handles_empty_message(capsys):
    t = Task(name="EmptyLog")
    t.log("")
    out = capsys.readouterr().out
    assert out == "[EmptyLog] \n"
