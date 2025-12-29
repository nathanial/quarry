/-
  Quarry Test Suite

  This file imports all test modules and runs them.
-/
import Crucible
import Tests.Database
import Tests.Binding
import Tests.Row
import Tests.ErrorHandling
import Tests.Config
import Tests.UserFunctions
import Tests.Backup
import Tests.VirtualTable
import Tests.Extensions
import Tests.Blob
import Tests.Hook
import Tests.Serialize
import Tests.Chisel

open Crucible

def main : IO UInt32 := do
  IO.println "Quarry Library Tests"
  IO.println "===================="
  runAllSuites
