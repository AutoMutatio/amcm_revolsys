param(
)

$scmConfig = Get-Content "$PSScriptRoot/Config.json" | ConvertFrom-Json

try {
  Push-Location -Path "$PSScriptRoot/../.."
 
  foreach ($repository in $scmConfig.repositories) {
    Write-Output $repository.path
    Write-Output "-  origin"
    git -C $repository.path fetch 'origin'
    foreach ($remote in $repository.remotes.psobject.properties.name) {
      if ('origin' -ne $remote) {
        Write-Output "-  $remote"
        git -C $repository.path fetch $remote
      }
    }
    Write-Output "-  origin tags"
    git -C $repository.path fetch 'origin' --tags  
  }
}
finally {
  Pop-Location
}
