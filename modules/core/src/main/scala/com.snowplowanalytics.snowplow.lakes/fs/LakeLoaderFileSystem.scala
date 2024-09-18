/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CreateFlag, FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.util.EnumSet
import java.util.concurrent.{ExecutorService, Executors}

/**
 * A hadoop FileSystem that wraps a delegate FileSystem
 *
 * All FileSystem methods are delegated to the 3rd-party FileSytem, e.g. s3a, gs, abfs FileSystem
 *
 * ...apart from `delete`. See description below.
 *
 * This customized FileSystem is needed so that `delete` tasks run asynchronously. Delta tables
 * occasionally require to delete a very large number of files, e.g. when periodically cleaning up
 * old log files. We want to avoid blocking the loader's main fibers of execution.
 *
 * It is safe to run `delete` tasks asynchronously because Open Table Formats do not have a hard
 * requirement that files are deleted immediately.
 */
class LakeLoaderFileSystem extends FileSystem {

  // ExecutorService for running `delete` tasks asynchronously
  private var executor: ExecutorService = _

  // The delegate FileSystem
  private var delegate: FileSystem = _

  lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  override def initialize(name: URI, conf: Configuration): Unit = {
    val scheme       = name.getScheme
    val delegateConf = new Configuration(conf)
    delegateConf.setBoolean(s"fs.$scheme.impl.disable.cache", true)
    Option(conf.get(s"fs.$scheme.lakeloader.delegate.impl")) match {
      case Some(impl) => delegateConf.set(s"fs.$scheme.impl", impl)
      case None       => delegateConf.unset(s"fs.$scheme.impl")
    }
    delegate = FileSystem.get(name, delegateConf)
    logger.debug(s"Initializing filesystem for $name with delegate ${delegate.getClass}")
    super.initialize(name, conf)
    executor = Executors.newSingleThreadExecutor()
  }

  override def close(): Unit = {
    executor.shutdown()
    super.close()
    delegate.close()
  }

  /**
   * Implements the Filesystem `delete` method by asynchronously calling the delegate FileSystem
   *
   * This implementation always returns `true`, which tells the caller that the file was deleted
   * successfully. The act of deleting the file is done asynchronously on a different thread. Any
   * problem deleting the file is ignored.
   */
  override def delete(f: Path, recursive: Boolean): Boolean = {
    executor.submit { () =>
      logger.debug(s"Asynchronously deleting $f")
      try
        delegate.delete(f, recursive)
      catch {
        case t: Throwable =>
          logger.info(s"Error deleting file $f: ${t.getMessage}")
      }
    }
    true
  }

  // The following methods are delegated entirely to the delegate FS

  override def create(
    f: Path,
    permission: FsPermission,
    overwrite: Boolean,
    bufferSize: Int,
    replication: Short,
    blockSize: Long,
    progress: Progressable
  ): FSDataOutputStream =
    delegate.create(f, permission, overwrite, bufferSize, replication, blockSize, progress)

  override def getUri(): URI =
    delegate.getUri()

  override def getWorkingDirectory(): Path =
    delegate.getWorkingDirectory()

  override def append(
    f: Path,
    bufferSize: Int,
    progress: Progressable
  ): FSDataOutputStream =
    delegate.append(f, bufferSize, progress)

  override def create(
    f: Path,
    permission: FsPermission,
    flags: EnumSet[CreateFlag],
    bufferSize: Int,
    replication: Short,
    blockSize: Long,
    progress: Progressable
  ): FSDataOutputStream =
    delegate.create(f, permission, flags, bufferSize, replication, blockSize, progress)

  override def getFileStatus(f: Path): FileStatus =
    delegate.getFileStatus(f)

  override def listStatus(f: Path): Array[FileStatus] =
    delegate.listStatus(f)

  override def mkdirs(f: Path, permission: FsPermission): Boolean =
    delegate.mkdirs(f, permission)

  override def open(path: Path, bufferSize: Int): FSDataInputStream =
    delegate.open(path, bufferSize)

  override def rename(src: Path, dst: Path): Boolean =
    delegate.rename(src, dst)

  override def setWorkingDirectory(path: Path): Unit =
    delegate.setWorkingDirectory(path)

  override def getScheme(): String =
    delegate.getScheme()

}
