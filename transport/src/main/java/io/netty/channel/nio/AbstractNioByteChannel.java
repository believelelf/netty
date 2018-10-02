/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.FileRegion;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.internal.ChannelUtils;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.util.internal.StringUtil;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

import static io.netty.channel.internal.ChannelUtils.WRITE_STATUS_SNDBUF_FULL;

/**
 * {@link AbstractNioChannel} base class for {@link Channel}s that operate on bytes.
 */
public abstract class AbstractNioByteChannel extends AbstractNioChannel {
    private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);
    private static final String EXPECTED_TYPES =
            " (expected: " + StringUtil.simpleClassName(ByteBuf.class) + ", " +
            StringUtil.simpleClassName(FileRegion.class) + ')';

    private final Runnable flushTask = new Runnable() {
        @Override
        public void run() {
            // Calling flush0 directly to ensure we not try to flush messages that were added via write(...) in the
            // meantime.
            ((AbstractNioUnsafe) unsafe()).flush0();
        }
    };
    private boolean inputClosedSeenErrorOnRead;

    /**
     * Create a new instance
     *
     * @param parent            the parent {@link Channel} by which this instance was created. May be {@code null}
     * @param ch                the underlying {@link SelectableChannel} on which it operates
     */
    protected AbstractNioByteChannel(Channel parent, SelectableChannel ch) {
        super(parent, ch, SelectionKey.OP_READ);
    }

    /**
     * Shutdown the input side of the channel.
     */
    protected abstract ChannelFuture shutdownInput();

    protected boolean isInputShutdown0() {
        return false;
    }

    @Override
    protected AbstractNioUnsafe newUnsafe() {
        return new NioByteUnsafe();
    }

    @Override
    public ChannelMetadata metadata() {
        return METADATA;
    }

    final boolean shouldBreakReadReady(ChannelConfig config) {
        return isInputShutdown0() && (inputClosedSeenErrorOnRead || !isAllowHalfClosure(config));
    }

    private static boolean isAllowHalfClosure(ChannelConfig config) {
        return config instanceof SocketChannelConfig &&
                ((SocketChannelConfig) config).isAllowHalfClosure();
    }

    protected class NioByteUnsafe extends AbstractNioUnsafe {

        private void closeOnRead(ChannelPipeline pipeline) {
            if (!isInputShutdown0()) {
                if (isAllowHalfClosure(config())) {
                    shutdownInput();
                    pipeline.fireUserEventTriggered(ChannelInputShutdownEvent.INSTANCE);
                } else {
                    close(voidPromise());
                }
            } else {
                inputClosedSeenErrorOnRead = true;
                pipeline.fireUserEventTriggered(ChannelInputShutdownReadComplete.INSTANCE);
            }
        }

        private void handleReadException(ChannelPipeline pipeline, ByteBuf byteBuf, Throwable cause, boolean close,
                RecvByteBufAllocator.Handle allocHandle) {
            // 未完成数据处理
            if (byteBuf != null) {
                if (byteBuf.isReadable()) {
                    readPending = false;
                    pipeline.fireChannelRead(byteBuf);
                } else {
                    byteBuf.release();
                }
            }
            // 发布事件
            allocHandle.readComplete();
            pipeline.fireChannelReadComplete();
            pipeline.fireExceptionCaught(cause);
            if (close || cause instanceof IOException) {
                // 处理统一事件
                closeOnRead(pipeline);
            }
        }

        @Override
        public final void read() {
            // 获取NioSocketChannel的SocketChannelConfig,主用用于设置客户端连接的TCP参数
            final ChannelConfig config = config();
            if (shouldBreakReadReady(config)) {
                //Set read pending to {@code false}.
                clearReadPending();
                return;
            }
            // 获取ChannelPipeline
            final ChannelPipeline pipeline = pipeline();
            // 从SocketChannelConfig中获取默认的allocator，并创建allocHandle
            final ByteBufAllocator allocator = config.getAllocator();
            final RecvByteBufAllocator.Handle allocHandle = recvBufAllocHandle();
            // 开始循环读之前，重设totalMessages等
            allocHandle.reset(config);
            //RecvByteBufAllocator 主要有两个实现类：FixedRecvByteBufAllocator及AdaptiveRecvByteBufAllocator
            //AdaptiveRecvByteBufAllocator是个缓冲区大小可以动态调整的ByteBuf分配器，内部定义了长度的向量表SIZE_TABLE，用于动态调整逻辑
            ByteBuf byteBuf = null;
            boolean close = false;
            try {
                do {
                    // 动态分配一个ByteBuf，初始容量1024字节，容量来自于Handle.guess()方法
                    byteBuf = allocHandle.allocate(allocator);
                    //Set the bytes that have been read for the last read operation.
                    // doReadBytes 由NioSocketChannel实现，从NIO SocketChannel读取消息到ByteBuf中
                    allocHandle.lastBytesRead(doReadBytes(byteBuf));
                    // 小于或等于0，连接关闭或未读取到消息
                    if (allocHandle.lastBytesRead() <= 0) {
                        // nothing was read. release the buffer.
                        byteBuf.release();
                        byteBuf = null;
                        // 连接关闭
                        close = allocHandle.lastBytesRead() < 0;
                        if (close) {
                            // There is nothing left to read as we received an EOF.
                            readPending = false;
                        }
                        break;
                    }

                    // 读取到了消息
                    allocHandle.incMessagesRead(1);
                    readPending = false;
                    // 完成一次异步读之后，就会触发一次ChannelRead事件。并不意味着读取了一条完整的消息，需要ChannelHandler进行半包处理。
                    pipeline.fireChannelRead(byteBuf);
                    byteBuf = null;
                    // 根据allocHandle规则，判断是否继续循环读。见MaxMessageHandle.continueReading
                } while (allocHandle.continueReading());

                // read完成，触发事件
                allocHandle.readComplete();
                pipeline.fireChannelReadComplete();

                // 对方连接关闭，关闭本方Channel
                if (close) {
                    closeOnRead(pipeline);
                }
            } catch (Throwable t) {
                // 统一异常处理
                handleReadException(pipeline, byteBuf, t, close, allocHandle);
            } finally {
                // Check if there is a readPending which was not processed yet.
                // This could be for two reasons:
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelRead(...) method
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelReadComplete(...) method
                //
                // See https://github.com/netty/netty/issues/2254
                if (!readPending && !config.isAutoRead()) {
                    removeReadOp();
                }
            }
        }
    }

    /**
     * Write objects to the OS.
     * @param in the collection which contains objects to write.
     * @return The value that should be decremented from the write quantum which starts at
     * {@link ChannelConfig#getWriteSpinCount()}. The typical use cases are as follows:
     * <ul>
     *     <li>0 - if no write was attempted. This is appropriate if an empty {@link ByteBuf} (or other empty content)
     *     is encountered</li>
     *     <li>1 - if a single call to write data was made to the OS</li>
     *     <li>{@link ChannelUtils#WRITE_STATUS_SNDBUF_FULL} - if an attempt to write data was made to the OS, but no
     *     data was accepted</li>
     * </ul>
     * @throws Exception if an I/O exception occurs during write.
     */
    protected final int doWrite0(ChannelOutboundBuffer in) throws Exception {
        Object msg = in.current();
        if (msg == null) {
            // Directly return here so incompleteWrite(...) is not called.
            return 0;
        }
        return doWriteInternal(in, in.current());
    }

    private int doWriteInternal(ChannelOutboundBuffer in, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            // 如果当前消息的可读字节数为0，说明当前消息不可读，进行丢弃，返回未处理0
            if (!buf.isReadable()) { // readerIndex> writerIndex --> false
               // 从环形数组中移除
                in.remove();
                return 0;
            }
            // 调用子类实现doWriteBytes进行消息发送
            // 1）如果本次发送的字节数为0，说明发送TCP缓冲区已满，发生了ZERO_WINDOW(滑动窗口为0)，
            // 返回WRITE_STATUS_SNDBUF_FULL（Integer.MAX_VALUE）,直接退出循环，setOpWrite=true，释放I/O线程,等待后续轮询
            // doWriteBytes在NioSocketChannel的实现，写入java.nio.channel中 buf.readBytes(javaChannel(), expectedWrittenBytes);
            final int localFlushedAmount = doWriteBytes(buf);
            // 2）如果发送字节数大于0，则对发送总数进行计数，
            //  判断当前消息是否已经发送成功（缓冲区没有可读字节），如果发送成功就是从环形数组中移除对当前ByteBuf
            // 如果没有，则保留待后续继续发送
            if (localFlushedAmount > 0) {
                // 调用ChannelOutboundBuffer更新发送进度信息
                in.progress(localFlushedAmount);
                // 判断是否发送成功
                if (!buf.isReadable()) {
                    // 成功，移除
                    in.remove();
                }
                // 当前次发送计数1
                return 1;
            }
        } else if (msg instanceof FileRegion) {
            FileRegion region = (FileRegion) msg;
            if (region.transferred() >= region.count()) {
                in.remove();
                return 0;
            }

            long localFlushedAmount = doWriteFileRegion(region);
            if (localFlushedAmount > 0) {
                in.progress(localFlushedAmount);
                if (region.transferred() >= region.count()) {
                    in.remove();
                }
                return 1;
            }
        } else {
            // Should not reach here.
            throw new Error();
        }
        // 缓冲区满
        return WRITE_STATUS_SNDBUF_FULL;
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        // 取得配置中的最大循环发送次数
        int writeSpinCount = config().getWriteSpinCount();
        do {
            // 从当前ChannelOutboundBuffer（环形数组）中弹出一条消息
            Object msg = in.current();
            if (msg == null) {
                // 如果消息为空，说明消息发送数组所有待发送消息已经发送完成，清除半包标识，并退出循环
                // Wrote all messages.
                clearOpWrite();
                // Directly return here so incompleteWrite(...) is not called.
                return;
            }
            // 消息不为空，进行消息处理，循环总数减去处理结果，有可能当前处理没有写，返回0的情况，后续要进行半包处理
            writeSpinCount -= doWriteInternal(in, msg);
        } while (writeSpinCount > 0);

        // 进行半包处理，
        incompleteWrite(writeSpinCount < 0);
    }

    @Override
    protected final Object filterOutboundMessage(Object msg) {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            if (buf.isDirect()) {
                return msg;
            }

            return newDirectBuffer(buf);
        }

        if (msg instanceof FileRegion) {
            return msg;
        }

        throw new UnsupportedOperationException(
                "unsupported message type: " + StringUtil.simpleClassName(msg) + EXPECTED_TYPES);
    }

    protected final void incompleteWrite(boolean setOpWrite) {
        // Did not write completely.
        if (setOpWrite) {
            // 设置OP_WRITE,等待多路复用器Selector不断轮询对应的Channel，用于处理没有发送完成的半包消息。（ZERO_WINDOW 防止CPU空转）
            setOpWrite();
        } else {
            // It is possible that we have set the write OP, woken up by NIO because the socket is writable, and then
            // use our write quantum. In this case we no longer want to set the write OP because the socket is still
            // writable (as far as we know). We will find out next time we attempt to write if the socket is writable
            // and set the write OP if necessary.
            clearOpWrite();
            // 如果没有设置OP_WRITE操作位，需要启动独立的Runnable,将其加入到EventLoop中执行，
            // 由Runnable负责半包消息的发送，实现方法为调用flush0()方法来发送缓存数组的消息。
            // 即异步发起下一次doWrite(ChannelOutboundBuffer in)操作
            // Schedule flush again later so other tasks can be picked up in the meantime
            eventLoop().execute(flushTask);
        }
    }

    /**
     * Write a {@link FileRegion}
     *
     * @param region        the {@link FileRegion} from which the bytes should be written
     * @return amount       the amount of written bytes
     */
    protected abstract long doWriteFileRegion(FileRegion region) throws Exception;

    /**
     * Read bytes into the given {@link ByteBuf} and return the amount.
     */
    protected abstract int doReadBytes(ByteBuf buf) throws Exception;

    /**
     * Write bytes form the given {@link ByteBuf} to the underlying {@link java.nio.channels.Channel}.
     * @param buf           the {@link ByteBuf} from which the bytes should be written
     * @return amount       the amount of written bytes
     */
    protected abstract int doWriteBytes(ByteBuf buf) throws Exception;

    protected final void setOpWrite() {
        final SelectionKey key = selectionKey();
        // Check first if the key is still valid as it may be canceled as part of the deregistration
        // from the EventLoop
        // See https://github.com/netty/netty/issues/2104
        if (!key.isValid()) {
            return;
        }
        final int interestOps = key.interestOps();
        if ((interestOps & SelectionKey.OP_WRITE) == 0) {
            key.interestOps(interestOps | SelectionKey.OP_WRITE);
        }
    }

    protected final void clearOpWrite() {
        final SelectionKey key = selectionKey();
        // Check first if the key is still valid as it may be canceled as part of the deregistration
        // from the EventLoop
        // See https://github.com/netty/netty/issues/2104
        if (!key.isValid()) {
            return;
        }
        // 获取当前标识位，判断是否注册了OP_WRITE,如注册了，进行位与反OP_WRITE运算，清除OP_WRITE标识位
        final int interestOps = key.interestOps();
        if ((interestOps & SelectionKey.OP_WRITE) != 0) {
            key.interestOps(interestOps & ~SelectionKey.OP_WRITE);
        }
    }
}
