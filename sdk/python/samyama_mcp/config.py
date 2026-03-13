"""Tool configuration via YAML or defaults."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CustomTool:
    """A user-defined tool backed by a Cypher template."""

    name: str
    description: str
    cypher_template: str
    parameters: list[dict] = field(default_factory=list)


@dataclass
class ToolConfig:
    """Controls which tool generators are active and any exclusions."""

    include_node_tools: bool = True
    include_edge_tools: bool = True
    include_vector_tools: bool = True
    include_algorithm_tools: bool = True
    exclude_labels: list[str] = field(default_factory=list)
    custom_tools: list[CustomTool] = field(default_factory=list)

    @classmethod
    def default(cls) -> ToolConfig:
        return cls()

    @classmethod
    def from_yaml(cls, path: str) -> ToolConfig:
        """Load configuration from a YAML file."""
        import yaml  # deferred import — only needed when config file is used

        with open(path) as fh:
            data = yaml.safe_load(fh) or {}

        custom = [
            CustomTool(
                name=t["name"],
                description=t.get("description", ""),
                cypher_template=t["cypher_template"],
                parameters=t.get("parameters", []),
            )
            for t in data.get("custom_tools", [])
        ]

        return cls(
            include_node_tools=data.get("include_node_tools", True),
            include_edge_tools=data.get("include_edge_tools", True),
            include_vector_tools=data.get("include_vector_tools", True),
            include_algorithm_tools=data.get("include_algorithm_tools", True),
            exclude_labels=data.get("exclude_labels", []),
            custom_tools=custom,
        )
