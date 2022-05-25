
## Contributing

See [issues](https://github.com/hhunterzinck/genie-bpc-quac/issues) to create a new issue or assign yourself to a task.


### Fork and clone this repository

[Fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) and [clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) the repository to your local machine.

Add this repository as an [upstream remote](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/configuring-a-remote-for-a-fork) on your local git repository and verify addition.

```
git remote add upstream git@github.com:hhunterzinck/genie-bpc-quac.git
git remote -v
```

On your local machine make sure you have the latest version of the `develop` branch:

```
git checkout develop
git pull upstream develop
```

### Development cycle

`genie-bpc-quac` follows the standard [GitHub flow](https://docs.github.com/en/get-started/quickstart/github-flow) development strategy.

1. Navigate to your local cloned repository.
1. Make sure your `develop` branch is up to date.

    ```
    cd {your-github-username}/genie-bpc-quac
    git checkout develop
    git pull upstream develop
    ```

1. Create a feature branch:

    ```
    git checkout develop
    git checkout -b gh-123-add-some-new-feature
    ```

1. Push branch to your fork on GitHub.

    ```
    git push --set-upstream origin gh-123-add-some-new-feature
    ```

1. Commit and push changes.

    ```
    git commit changed_file.txt -m "Remove X parameter because it was unused"
    git push
    ```


1. On Github, create a pull request from the feature branch of your fork to the `develop` branch of hhunterzinck/genie-bpc-quac.  Make sure to add 'Fixes #{issue_number}' to the comment of your pull request to automatically link the issue.  

### DockerHub build

The [sagebionetworks/genie-bpc-quac](https://hub.docker.com/repository/docker/sagebionetworks/genie-bpc-quac) docker image is automatically built everytime a change to pushed into the `develop` branch.  This is automatically set up via the automated build feature on DockerHub.
