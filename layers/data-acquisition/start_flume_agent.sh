#!/bin/sh

flume-ng agent --conf-file flumeAgentConfig.properties --name agentBD -Dflume.root.logger=INFO,console
