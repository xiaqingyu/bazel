// Copyright 2017 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.devtools.build.lib.remote;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.OutputDirectory;
import build.bazel.remote.execution.v2.OutputFile;
import build.bazel.remote.execution.v2.OutputSymlink;
import build.bazel.remote.execution.v2.Tree;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.Artifact.SpecialArtifact;
import com.google.devtools.build.lib.actions.EnvironmentalExecException;
import com.google.devtools.build.lib.actions.ExecException;
import com.google.devtools.build.lib.actions.FileArtifactValue.RemoteFileArtifactValue;
import com.google.devtools.build.lib.actions.UserExecException;
import com.google.devtools.build.lib.actions.cache.MetadataInjector;
import com.google.devtools.build.lib.concurrent.ThreadSafety;
import com.google.devtools.build.lib.profiler.Profiler;
import com.google.devtools.build.lib.profiler.SilentCloseable;
import com.google.devtools.build.lib.remote.AbstractRemoteActionCache.ActionOutputMetadata.OutputFileMetadata;
import com.google.devtools.build.lib.remote.AbstractRemoteActionCache.ActionOutputMetadata.OutputSymlinkMetadata;
import com.google.devtools.build.lib.remote.TreeNodeRepository.TreeNode;
import com.google.devtools.build.lib.remote.options.RemoteOptions;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.remote.util.Utils;
import com.google.devtools.build.lib.util.io.FileOutErr;
import com.google.devtools.build.lib.vfs.Dirent;
import com.google.devtools.build.lib.vfs.FileStatus;
import com.google.devtools.build.lib.vfs.FileSystemUtils;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.devtools.build.lib.vfs.Symlinks;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Context;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/** A cache for storing artifacts (input and output) as well as the output of running an action. */
@ThreadSafety.ThreadSafe
abstract class AbstractRemoteActionCache implements AutoCloseable {

  private static final ListenableFuture<Void> COMPLETED_SUCCESS = SettableFuture.create();
  private static final ListenableFuture<byte[]> EMPTY_BYTES = SettableFuture.create();

  static {
    ((SettableFuture<Void>) COMPLETED_SUCCESS).set(null);
    ((SettableFuture<byte[]>) EMPTY_BYTES).set(new byte[0]);
  }

  public static boolean causedByCacheMiss(IOException t) {
    return t.getCause() instanceof CacheNotFoundException;
  }

  protected final RemoteOptions options;
  protected final DigestUtil digestUtil;
  private final Retrier retrier;

  public AbstractRemoteActionCache(RemoteOptions options, DigestUtil digestUtil, Retrier retrier) {
    this.options = options;
    this.digestUtil = digestUtil;
    this.retrier = retrier;
  }

  /**
   * Ensures that the tree structure of the inputs, the input files themselves, and the command are
   * available in the remote cache, such that the tree can be reassembled and executed on another
   * machine given the root digest.
   *
   * <p>The cache may check whether files or parts of the tree structure are already present, and do
   * not need to be uploaded again.
   *
   * <p>Note that this method is only required for remote execution, not for caching itself.
   * However, remote execution uses a cache to store input files, and that may be a separate
   * end-point from the executor itself, so the functionality lives here. A pure remote caching
   * implementation that does not support remote execution may choose not to implement this
   * function, and throw {@link UnsupportedOperationException} instead. If so, it should be clearly
   * documented that it cannot be used for remote execution.
   */
  public abstract void ensureInputsPresent(
      TreeNodeRepository repository, Path execRoot, TreeNode root, Action action, Command command)
      throws IOException, InterruptedException;

  /**
   * Attempts to look up the given action in the remote cache and return its result, if present.
   * Returns {@code null} if there is no such entry. Note that a successful result from this method
   * does not guarantee the availability of the corresponding output files in the remote cache.
   *
   * @throws IOException if the remote cache is unavailable.
   */
  abstract @Nullable ActionResult getCachedActionResult(DigestUtil.ActionKey actionKey)
      throws IOException, InterruptedException;

  /**
   * Upload the result of a locally executed action to the cache by uploading any necessary files,
   * stdin / stdout, as well as adding an entry for the given action key to the cache if
   * uploadAction is true.
   *
   * @throws IOException if the remote cache is unavailable.
   */
  abstract void upload(
      DigestUtil.ActionKey actionKey,
      Action action,
      Command command,
      Path execRoot,
      Collection<Path> files,
      FileOutErr outErr,
      boolean uploadAction)
      throws ExecException, IOException, InterruptedException;

  /**
   * Downloads a blob with a content hash {@code digest} to {@code out}.
   *
   * @return a future that completes after the download completes (succeeds / fails).
   */
  protected abstract ListenableFuture<Void> downloadBlob(Digest digest, OutputStream out);

  /**
   * Downloads a blob with content hash {@code digest} and stores its content in memory.
   *
   * @return a future that completes after the download completes (succeeds / fails). If successful,
   *     the content is stored in the future's {@code byte[]}.
   */
  public ListenableFuture<byte[]> downloadBlob(Digest digest) {
    if (digest.getSizeBytes() == 0) {
      return EMPTY_BYTES;
    }
    ByteArrayOutputStream bOut = new ByteArrayOutputStream((int) digest.getSizeBytes());
    SettableFuture<byte[]> outerF = SettableFuture.create();
    Futures.addCallback(
        downloadBlob(digest, bOut),
        new FutureCallback<Void>() {
          @Override
          public void onSuccess(Void aVoid) {
            outerF.set(bOut.toByteArray());
          }

          @Override
          public void onFailure(Throwable t) {
            outerF.setException(t);
          }
        },
        directExecutor());
    return outerF;
  }

  /**
   * Value class for action output metadata.
   */
  static class ActionOutputMetadata {

    static class OutputSymlinkMetadata {
      private final Path path;
      private final String target;

      public OutputSymlinkMetadata(Path path, String target) {
        this.path = path;
        this.target = target;
      }

      public Path path() {
        return path;
      }

      public String target() {
        return target;
      }
    }

    static class OutputFileMetadata {
      private final Path path;
      private final byte[] digest;
      private final long sizeBytes;
      private final boolean isExecutable;

      public OutputFileMetadata(Path path, byte[] digest, long sizeBytes, boolean isExecutable) {
        this.path = path;
        this.digest = digest;
        this.sizeBytes = sizeBytes;
        this.isExecutable = isExecutable;
      }

      public Path path() {
        return path;
      }

      public byte[] digest() {
        return digest;
      }

      public long sizeBytes() {
        return sizeBytes;
      }

      public boolean isExecutable() {
        return isExecutable;
      }
    }

    private final ImmutableMap<Path, OutputFileMetadata> filesMetadata;
    private final ImmutableMap<Path, OutputSymlinkMetadata> symlinksMetadata;
    private final ImmutableMap<Path, ImmutableList<OutputFileMetadata>> directoriesMetadata;

    public ActionOutputMetadata(
        ImmutableMap<Path, OutputFileMetadata> filesMetadata,
        ImmutableMap<Path, OutputSymlinkMetadata> symlinksMetadata,
        ImmutableMap<Path, ImmutableList<OutputFileMetadata>> directoriesMetadata) {
      this.filesMetadata = filesMetadata;
      this.symlinksMetadata = symlinksMetadata;
      this.directoriesMetadata = directoriesMetadata;
    }

    @Nullable
    public OutputFileMetadata fileMetadata(Path path) {
      return filesMetadata.get(path);
    }

    @Nullable
    public OutputSymlinkMetadata symlinkMetadata(Path path) {
      return symlinksMetadata.get(path);
    }

    @Nullable
    public ImmutableList<OutputFileMetadata> treeArtifactMetadata(Path path) {
      return directoriesMetadata.get(path);
    }

    public Collection<OutputFileMetadata> filesMetadata() {
      return filesMetadata.values();
    }

    public Collection<ImmutableList<OutputFileMetadata>> directoriesMetadata() {
      return directoriesMetadata.values();
    }

    public Collection<OutputSymlinkMetadata> symlinksMetadata() {
      return symlinksMetadata.values();
    }
  }

  public ImmutableList<OutputFileMetadata> computeFilesInDirectory(
      Path path, Directory dir, Map<Digest, Directory> childDirectoriesMap) {
    ImmutableList.Builder<OutputFileMetadata> filesBuilder = ImmutableList.builder();
    for (FileNode file : dir.getFilesList()) {
      byte[] digest = HashCode.fromString(file.getDigest().getHash()).asBytes();
      filesBuilder.add(
          new OutputFileMetadata(
              path.getRelative(file.getName()),
              digest,
              file.getDigest().getSizeBytes(),
              file.getIsExecutable()));
    }

    for (DirectoryNode directoryNode : dir.getDirectoriesList()) {
      Path childPath = path.getRelative(directoryNode.getName());
      Directory childDir =
          Preconditions.checkNotNull(childDirectoriesMap.get(directoryNode.getDigest()));
      filesBuilder.addAll(computeFilesInDirectory(childPath, childDir, childDirectoriesMap));
    }

    return filesBuilder.build();
  }

  private ActionOutputMetadata retrieveOutputMetadata(ActionResult actionResult, Path execRoot)
      throws IOException, InterruptedException {
    Preconditions.checkNotNull(actionResult, "actionResult must not be null");
    Context ctx = Context.current();
    Map<Path, ListenableFuture<Tree>> dirMetadataDownloads =
        Maps.newHashMapWithExpectedSize(actionResult.getOutputDirectoriesCount());
    for (OutputDirectory dir : actionResult.getOutputDirectoriesList()) {
      dirMetadataDownloads.put(
          execRoot.getRelative(dir.getPath()),
          Futures.transform(
              retrier.executeAsync(() -> ctx.call(() -> downloadBlob(dir.getTreeDigest()))),
              (treeBytes) -> {
                try {
                  return Tree.parseFrom(treeBytes);
                } catch (InvalidProtocolBufferException e) {
                  throw new RuntimeException(e);
                }
              },
              directExecutor()));
    }

    ImmutableMap.Builder<Path, ImmutableList<OutputFileMetadata>> outputDirectoriesMetadata =
        ImmutableMap.builder();
    for (Map.Entry<Path, ListenableFuture<Tree>> metadataDownload :
        dirMetadataDownloads.entrySet()) {
      Path path = metadataDownload.getKey();
      Tree directoryTree = getFromFuture(metadataDownload.getValue());
      Map<Digest, Directory> childrenMap = new HashMap<>();
      for (Directory childDir : directoryTree.getChildrenList()) {
        childrenMap.put(digestUtil.compute(childDir), childDir);
      }

      outputDirectoriesMetadata.put(
          path, computeFilesInDirectory(path, directoryTree.getRoot(), childrenMap));
    }

    ImmutableMap.Builder<Path, OutputFileMetadata> outputFilesMetadata = ImmutableMap.builder();
    for (OutputFile outputFile : actionResult.getOutputFilesList()) {
      byte[] digest = HashCode.fromString(outputFile.getDigest().getHash()).asBytes();
      outputFilesMetadata.put(
          execRoot.getRelative(outputFile.getPath()),
          new OutputFileMetadata(
              execRoot.getRelative(outputFile.getPath()),
              digest,
              outputFile.getDigest().getSizeBytes(),
              outputFile.getIsExecutable()));
    }

    ImmutableMap.Builder<Path, OutputSymlinkMetadata> outputSymlinksMetadata =
        ImmutableMap.builder();
    Iterable<OutputSymlink> outputSymlinks =
        Iterables.concat(
            actionResult.getOutputFileSymlinksList(),
            actionResult.getOutputDirectorySymlinksList());
    for (OutputSymlink symlink : outputSymlinks) {
      outputSymlinksMetadata.put(
          execRoot.getRelative(symlink.getPath()),
          new OutputSymlinkMetadata(execRoot.getRelative(symlink.getPath()), symlink.getTarget()));
    }

    return new ActionOutputMetadata(
        outputFilesMetadata.build(),
        outputSymlinksMetadata.build(),
        outputDirectoriesMetadata.build());
  }

  /**
   * Download the output files and directory trees of a remotely executed action to the local
   * machine, as well stdin / stdout to the given files.
   *
   * <p>In case of failure, this method deletes any output files it might have already created.
   *
   * @throws IOException in case of a cache miss or if the remote cache is unavailable.
   * @throws ExecException in case clean up after a failed download failed.
   */
  // TODO(olaola): will need to amend to include the TreeNodeRepository for updating.
  public void download(ActionResult result, Path execRoot, FileOutErr outErr)
      throws ExecException, IOException, InterruptedException {
    Context ctx = Context.current();
    ActionOutputMetadata metadata = retrieveOutputMetadata(result, execRoot);

    List<ListenableFuture<OutputFileMetadata>> downloads =
        Stream.concat(
                metadata.filesMetadata().stream(),
                metadata.directoriesMetadata().stream().flatMap(Collection::stream))
            .map(
                (file) -> {
                  Digest digest = DigestUtil.buildDigest(file.digest(), file.sizeBytes());
                  ListenableFuture<Void> download =
                      retrier.executeAsync(() -> ctx.call(() -> downloadFile(file.path(), digest)));
                  return Futures.transform(download, (d) -> file, directExecutor());
                })
            .collect(Collectors.toList());

    // Subsequently we need to wait for *every* download to finish, even if we already know that
    // one failed. That's so that when exiting this method we can be sure that all downloads have
    // finished and don't race with the cleanup routine.
    // TODO(buchgr): Look into cancellation.

    IOException downloadException = null;
    InterruptedException interruptedException = null;
    try {
      downloads.addAll(downloadOutErr(result, outErr));
    } catch (IOException e) {
      downloadException = e;
    }

    for (ListenableFuture<OutputFileMetadata> download : downloads) {
      try {
        OutputFileMetadata outputFile = getFromFuture(download);
        if (outputFile != null) {
          outputFile.path().setExecutable(outputFile.isExecutable());
        }
      } catch (IOException e) {
        downloadException = downloadException == null ? e : downloadException;
      } catch (InterruptedException e) {
        interruptedException = interruptedException == null ? e : interruptedException;
      }
    }

    if (downloadException != null || interruptedException != null) {
      try {
        // Delete any (partially) downloaded output files, since any subsequent local execution
        // of this action may expect none of the output files to exist.
        for (OutputFile file : result.getOutputFilesList()) {
          execRoot.getRelative(file.getPath()).delete();
        }
        for (OutputDirectory directory : result.getOutputDirectoriesList()) {
          FileSystemUtils.deleteTree(execRoot.getRelative(directory.getPath()));
        }
        if (outErr != null) {
          outErr.getOutputPath().delete();
          outErr.getErrorPath().delete();
        }
      } catch (IOException e) {
        // If deleting of output files failed, we abort the build with a decent error message as
        // any subsequent local execution failure would likely be incomprehensible.

        // We don't propagate the downloadException, as this is a recoverable error and the cause
        // of the build failure is really that we couldn't delete output files.
        throw new EnvironmentalExecException(
            "Failed to delete output files after incomplete "
                + "download. Cannot continue with local execution.",
            e,
            true);
      }
    }

    if (interruptedException != null) {
      throw interruptedException;
    }

    if (downloadException != null) {
      throw downloadException;
    }

    // We create the symbolic links after all regular downloads are finished, because dangling
    // links will not work on Windows.
    createSymbolicLinks(
        execRoot,
        Iterables.concat(
            result.getOutputFileSymlinksList(), result.getOutputDirectorySymlinksList()));
  }

  // Creates a local symbolic link. Only relative symlinks are supported.
  private void createSymbolicLink(Path path, String target) throws IOException {
    PathFragment targetPath = PathFragment.create(target);
    if (targetPath.isAbsolute()) {
      // Error, we do not support absolute symlinks as outputs.
      throw new IOException(
          String.format(
              "Action output %s is a symbolic link to an absolute path %s. "
                  + "Symlinks to absolute paths in action outputs are not supported.",
              path, target));
    }
    path.createSymbolicLink(targetPath);
  }

  // Creates symbolic links locally as created remotely by the action. Only relative symbolic
  // links are supported, because absolute symlinks break action hermeticity.
  private void createSymbolicLinks(Path execRoot, Iterable<OutputSymlink> symlinks)
      throws IOException {
    for (OutputSymlink symlink : symlinks) {
      Path path = execRoot.getRelative(symlink.getPath());
      Preconditions.checkNotNull(
              path.getParentDirectory(), "Failed creating directory and parents for %s", path)
          .createDirectoryAndParents();
      createSymbolicLink(path, symlink.getTarget());
    }
  }

  /** Download a file (that is not a directory). The content is fetched from the digest. */
  public ListenableFuture<Void> downloadFile(Path path, Digest digest) throws IOException {
    Preconditions.checkNotNull(path.getParentDirectory()).createDirectoryAndParents();
    if (digest.getSizeBytes() == 0) {
      // Handle empty file locally.
      FileSystemUtils.writeContent(path, new byte[0]);
      return COMPLETED_SUCCESS;
    }

    OutputStream out = new LazyFileOutputStream(path);
    SettableFuture<Void> outerF = SettableFuture.create();
    ListenableFuture<Void> f = downloadBlob(digest, out);
    Futures.addCallback(
        f,
        new FutureCallback<Void>() {
          @Override
          public void onSuccess(Void result) {
            try {
              out.close();
              outerF.set(null);
            } catch (IOException e) {
              outerF.setException(e);
            }
          }

          @Override
          public void onFailure(Throwable t) {
            outerF.setException(t);
            try {
              out.close();
            } catch (IOException e) {
              // Intentionally left empty. The download already failed, so we can ignore
              // the error on close().
            }
          }
        },
        directExecutor());
    return outerF;
  }

  private List<ListenableFuture<OutputFileMetadata>> downloadOutErr(
      ActionResult result, FileOutErr outErr) throws IOException {
    Context ctx = Context.current();
    List<ListenableFuture<OutputFileMetadata>> downloads = new ArrayList<>();
    if (!result.getStdoutRaw().isEmpty()) {
      result.getStdoutRaw().writeTo(outErr.getOutputStream());
      outErr.getOutputStream().flush();
    } else if (result.hasStdoutDigest()) {
      downloads.add(
          Futures.transform(
              retrier.executeAsync(
                  () ->
                      ctx.call(
                          () -> downloadBlob(result.getStdoutDigest(), outErr.getOutputStream()))),
              (d) -> null,
              directExecutor()));
    }
    if (!result.getStderrRaw().isEmpty()) {
      result.getStderrRaw().writeTo(outErr.getErrorStream());
      outErr.getErrorStream().flush();
    } else if (result.hasStderrDigest()) {
      downloads.add(
          Futures.transform(
              retrier.executeAsync(
                  () ->
                      ctx.call(
                          () -> downloadBlob(result.getStderrDigest(), outErr.getErrorStream()))),
              (d) -> null,
              directExecutor()));
    }
    return downloads;
  }

  /** UploadManifest adds output metadata to a {@link ActionResult}. */
  static class UploadManifest {
    private final DigestUtil digestUtil;
    private final ActionResult.Builder result;
    private final Path execRoot;
    private final boolean allowSymlinks;
    private final boolean uploadSymlinks;
    private final Map<Digest, Path> digestToFile;
    private final Map<Digest, Chunker> digestToChunkers;

    /**
     * Create an UploadManifest from an ActionResult builder and an exec root. The ActionResult
     * builder is populated through a call to {@link #addFile(Digest, Path)}.
     */
    public UploadManifest(
        DigestUtil digestUtil,
        ActionResult.Builder result,
        Path execRoot,
        boolean uploadSymlinks,
        boolean allowSymlinks) {
      this.digestUtil = digestUtil;
      this.result = result;
      this.execRoot = execRoot;
      this.uploadSymlinks = uploadSymlinks;
      this.allowSymlinks = allowSymlinks;

      this.digestToFile = new HashMap<>();
      this.digestToChunkers = new HashMap<>();
    }

    /**
     * Add a collection of files or directories to the UploadManifest. Adding a directory has the
     * effect of 1) uploading a {@link Tree} protobuf message from which the whole structure of the
     * directory, including the descendants, can be reconstructed and 2) uploading all the
     * non-directory descendant files.
     */
    public void addFiles(Collection<Path> files) throws ExecException, IOException {
      for (Path file : files) {
        // TODO(ulfjack): Maybe pass in a SpawnResult here, add a list of output files to that, and
        // rely on the local spawn runner to stat the files, instead of statting here.
        FileStatus stat = file.statIfFound(Symlinks.NOFOLLOW);
        // TODO(#6547): handle the case where the parent directory of the output file is an
        // output symlink.
        if (stat == null) {
          // We ignore requested results that have not been generated by the action.
          continue;
        }
        if (stat.isDirectory()) {
          addDirectory(file);
        } else if (stat.isFile() && !stat.isSpecialFile()) {
          Digest digest = digestUtil.compute(file, stat.getSize());
          addFile(digest, file);
        } else if (stat.isSymbolicLink() && allowSymlinks) {
          PathFragment target = file.readSymbolicLink();
          // Need to resolve the symbolic link to know what to add, file or directory.
          FileStatus statFollow = file.statIfFound(Symlinks.FOLLOW);
          if (statFollow == null) {
            throw new IOException(
                String.format("Action output %s is a dangling symbolic link to %s ", file, target));
          }
          if (statFollow.isSpecialFile()) {
            illegalOutput(file);
          }
          Preconditions.checkState(
              statFollow.isFile() || statFollow.isDirectory(), "Unknown stat type for %s", file);
          if (uploadSymlinks && !target.isAbsolute()) {
            if (statFollow.isFile()) {
              addFileSymbolicLink(file, target);
            } else {
              addDirectorySymbolicLink(file, target);
            }
          } else {
            if (statFollow.isFile()) {
              addFile(digestUtil.compute(file), file);
            } else {
              addDirectory(file);
            }
          }
        } else {
          illegalOutput(file);
        }
      }
    }

    /**
     * Adds an action and command protos to upload. They need to be uploaded as part of the action
     * result.
     */
    public void addAction(DigestUtil.ActionKey actionKey, Action action, Command command)
        throws IOException {
      byte[] actionBlob = action.toByteArray();
      digestToChunkers.put(
          actionKey.getDigest(),
          Chunker.builder(digestUtil)
              .setInput(actionKey.getDigest(), actionBlob)
              .setChunkSize(actionBlob.length)
              .build());
      byte[] commandBlob = command.toByteArray();
      digestToChunkers.put(
          action.getCommandDigest(),
          Chunker.builder(digestUtil)
              .setInput(action.getCommandDigest(), commandBlob)
              .setChunkSize(commandBlob.length)
              .build());
    }

    /** Map of digests to file paths to upload. */
    public Map<Digest, Path> getDigestToFile() {
      return digestToFile;
    }

    /**
     * Map of digests to chunkers to upload. When the file is a regular, non-directory file it is
     * transmitted through {@link #getDigestToFile()}. When it is a directory, it is transmitted as
     * a {@link Tree} protobuf message through {@link #getDigestToChunkers()}.
     */
    public Map<Digest, Chunker> getDigestToChunkers() {
      return digestToChunkers;
    }

    private void addFileSymbolicLink(Path file, PathFragment target) throws IOException {
      result
          .addOutputFileSymlinksBuilder()
          .setPath(file.relativeTo(execRoot).getPathString())
          .setTarget(target.toString());
    }

    private void addDirectorySymbolicLink(Path file, PathFragment target) throws IOException {
      result
          .addOutputDirectorySymlinksBuilder()
          .setPath(file.relativeTo(execRoot).getPathString())
          .setTarget(target.toString());
    }

    private void addFile(Digest digest, Path file) throws IOException {
      result
          .addOutputFilesBuilder()
          .setPath(file.relativeTo(execRoot).getPathString())
          .setDigest(digest)
          .setIsExecutable(file.isExecutable());

      digestToFile.put(digest, file);
    }

    private void addDirectory(Path dir) throws ExecException, IOException {
      Tree.Builder tree = Tree.newBuilder();
      Directory root = computeDirectory(dir, tree);
      tree.setRoot(root);

      byte[] blob = tree.build().toByteArray();
      Digest digest = digestUtil.compute(blob);
      Chunker chunker =
          Chunker.builder(digestUtil).setInput(digest, blob).setChunkSize(blob.length).build();

      if (result != null) {
        result
            .addOutputDirectoriesBuilder()
            .setPath(dir.relativeTo(execRoot).getPathString())
            .setTreeDigest(digest);
      }

      digestToChunkers.put(chunker.digest(), chunker);
    }

    private Directory computeDirectory(Path path, Tree.Builder tree)
        throws ExecException, IOException {
      Directory.Builder b = Directory.newBuilder();

      List<Dirent> sortedDirent = new ArrayList<>(path.readdir(Symlinks.NOFOLLOW));
      sortedDirent.sort(Comparator.comparing(Dirent::getName));

      for (Dirent dirent : sortedDirent) {
        String name = dirent.getName();
        Path child = path.getRelative(name);
        if (dirent.getType() == Dirent.Type.DIRECTORY) {
          Directory dir = computeDirectory(child, tree);
          b.addDirectoriesBuilder().setName(name).setDigest(digestUtil.compute(dir));
          tree.addChildren(dir);
        } else if (dirent.getType() == Dirent.Type.SYMLINK && allowSymlinks) {
          PathFragment target = child.readSymbolicLink();
          if (uploadSymlinks && !target.isAbsolute()) {
            // Whether it is dangling or not, we're passing it on.
            b.addSymlinksBuilder().setName(name).setTarget(target.toString());
            continue;
          }
          // Need to resolve the symbolic link now to know whether to upload a file or a directory.
          FileStatus statFollow = child.statIfFound(Symlinks.FOLLOW);
          if (statFollow == null) {
            throw new IOException(
                String.format(
                    "Action output %s is a dangling symbolic link to %s ", child, target));
          }
          if (statFollow.isFile() && !statFollow.isSpecialFile()) {
            Digest digest = digestUtil.compute(child);
            b.addFilesBuilder()
                .setName(name)
                .setDigest(digest)
                .setIsExecutable(child.isExecutable());
            digestToFile.put(digest, child);
          } else if (statFollow.isDirectory()) {
            Directory dir = computeDirectory(child, tree);
            b.addDirectoriesBuilder().setName(name).setDigest(digestUtil.compute(dir));
            tree.addChildren(dir);
          } else {
            illegalOutput(child);
          }
        } else if (dirent.getType() == Dirent.Type.FILE) {
          Digest digest = digestUtil.compute(child);
          b.addFilesBuilder().setName(name).setDigest(digest).setIsExecutable(child.isExecutable());
          digestToFile.put(digest, child);
        } else {
          illegalOutput(child);
        }
      }

      return b.build();
    }

    private void illegalOutput(Path what) throws ExecException {
      String kind = what.isSymbolicLink() ? "symbolic link" : "special file";
      throw new UserExecException(
          String.format(
              "Output %s is a %s. Only regular files and directories may be "
                  + "uploaded to a remote cache. "
                  + "Change the file type or use --remote_allow_symlink_upload.",
              what.relativeTo(execRoot), kind));
    }
  }

  private void downloadRequiredLocalOutputs(Collection<? extends ActionInput> outputs,
      ActionOutputMetadata metadata, Path execRoot) throws IOException, InterruptedException {
    // TODO(buchgr): Implement support for tree artifacts
    List<ListenableFuture<OutputFileMetadata>> downloads = new ArrayList<>(outputs.size());
    for (ActionInput output : outputs) {
      Path path = execRoot.getRelative(output.getExecPath());
      OutputFileMetadata fileMetadata = metadata.fileMetadata(path);
      downloads.add(Futures.transform(
          downloadFile(path, DigestUtil.buildDigest(fileMetadata.digest(), fileMetadata.sizeBytes())),
          (d) -> fileMetadata, MoreExecutors.directExecutor()));
    }

    for (ListenableFuture<OutputFileMetadata> download : downloads) {
      getFromFuture(download);
    }
  }

  private void injectRemoteOutputArtifacts(
      Collection<Artifact> remoteOutputs,
      ActionOutputMetadata metadata,
      Path execRoot,
      MetadataInjector metadataInjector) {
    for (Artifact remoteOutput : remoteOutputs) {
      if (remoteOutput.isTreeArtifact()) {
        List<OutputFileMetadata> outputFilesMetadata =
            metadata.treeArtifactMetadata(execRoot.getRelative(remoteOutput.getExecPathString()));
        if (outputFilesMetadata == null) {
          // A declared output wasn't created. It might have been an optional output and if not
          // SkyFrame will make sure to fail.
          continue;
        }
        ImmutableMap.Builder<PathFragment, RemoteFileArtifactValue> childMetadata =
            ImmutableMap.builder();
        for (OutputFileMetadata outputFile : outputFilesMetadata) {
          childMetadata.put(
              outputFile.path().relativeTo(remoteOutput.getPath()),
              new RemoteFileArtifactValue(outputFile.digest(), outputFile.sizeBytes(), 1));
        }
        metadataInjector.injectRemoteDirectory(
            (SpecialArtifact) remoteOutput, childMetadata.build());
      } else {
        OutputFileMetadata outputMetadata =
            metadata.fileMetadata(execRoot.getRelative(remoteOutput.getExecPathString()));
        if (outputMetadata == null) {
          // A declared output wasn't created. It might have been an optional output and if not
          // SkyFrame will make sure to fail.
          continue;
        }
        metadataInjector.injectRemoteFile(
            remoteOutput, outputMetadata.digest(), outputMetadata.sizeBytes(), 1);
      }
    }
  }

  public void injectRemoteMetadata(ActionResult result,
      Collection<? extends ActionInput> outputs,
      Collection<? extends ActionInput> requiredLocalOutputs,
      FileOutErr outErr,
      Path execRoot,
      MetadataInjector metadataInjector) throws IOException, InterruptedException {
    Preconditions.checkState(result.getExitCode() == 0, "injecting remote metadata is only "
        + "supported for successful actions (exit code 0).");

    ActionOutputMetadata metadata;
    try (SilentCloseable c = Profiler.instance().profile("Remote.retrieveOutputMetadata")) {
      metadata = retrieveOutputMetadata(result, execRoot);
    }

    if (!metadata.symlinksMetadata().isEmpty()) {
      throw new IOException("Symlinks in action outputs are not yet supported by "
          + "--experimental_remote_fetch_outputs");
    }

    Map<PathFragment, ActionInput> outputsToDownload =
        Maps.newHashMapWithExpectedSize(requiredLocalOutputs.size());
    for (ActionInput output : requiredLocalOutputs) {
      outputsToDownload.put(output.getExecPath(), output);
    }
    int numOutputsToInject = outputs.size() - requiredLocalOutputs.size();
    List<Artifact> outputsToInject = new ArrayList<>(numOutputsToInject);
    for (ActionInput output : outputs) {
      if (outputsToDownload.containsKey(output.getExecPath()) || !(output instanceof Artifact)) {
        continue;
      }
      outputsToInject.add((Artifact) output);
    }

    try (SilentCloseable c = Profiler.instance().profile("Remote.downloadRequiredLocalOutputs")) {
      downloadRequiredLocalOutputs(requiredLocalOutputs, metadata, execRoot);
    }

    try (SilentCloseable c = Profiler.instance().profile("Remote.downloadStdoutStderr")) {
      for (ListenableFuture<OutputFileMetadata> download : downloadOutErr(result, outErr)) {
        getFromFuture(download);
      }
    }

    injectRemoteOutputArtifacts(outputsToInject, metadata, execRoot, metadataInjector);
  }


  /** Release resources associated with the cache. The cache may not be used after calling this. */
  @Override
  public abstract void close();

  /**
   * Creates an {@link OutputStream} that isn't actually opened until the first data is written.
   * This is useful to only have as many open file descriptors as necessary at a time to avoid
   * running into system limits.
   */
  private static class LazyFileOutputStream extends OutputStream {

    private final Path path;
    private OutputStream out;

    public LazyFileOutputStream(Path path) {
      this.path = path;
    }

    @Override
    public void write(byte[] b) throws IOException {
      ensureOpen();
      out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      ensureOpen();
      out.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
      ensureOpen();
      out.write(b);
    }

    @Override
    public void flush() throws IOException {
      ensureOpen();
      out.flush();
    }

    @Override
    public void close() throws IOException {
      ensureOpen();
      out.close();
    }

    private void ensureOpen() throws IOException {
      if (out == null) {
        out = path.getOutputStream();
      }
    }
  }

  @VisibleForTesting
  protected <T> T getFromFuture(ListenableFuture<T> f) throws IOException, InterruptedException {
    return Utils.getFromFuture(f);
  }
}
