#!/bin/sh

flume-ng agent --conf-file flumeAgentIDConfig.properties --name agentID -Dflume.root.logger=INFO,console