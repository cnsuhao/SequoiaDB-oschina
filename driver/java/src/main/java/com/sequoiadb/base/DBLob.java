/**
 *      Copyright (C) 2012 SequoiaDB Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
/**
 * @package com.sequoiadb.base;
 * @brief SequoiaDB Driver for Java
 * @author YouBin Lin
 */

package com.sequoiadb.base;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import com.sequoiadb.base.SequoiadbConstants.Operation;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.util.Helper;
import com.sequoiadb.util.SDBMessageHelper;

/**
 * @class DBLob
 * @brief Operation interfaces of DBLob.
 */
public interface DBLob {
    /**
     * @memberof SDB_LOB_SEEK_SET 0
     * @brief Change the position from the beginning of lob 
     */
    public final static int SDB_LOB_SEEK_SET   = 0;
    
    /**
     * @memberof SDB_LOB_SEEK_CUR 1
     * @brief Change the position from the current position of lob 
     */
    public final static int SDB_LOB_SEEK_CUR   = 1;
    
    /**
     * @memberof SDB_LOB_SEEK_END 2
     * @brief Change the position from the end of lob 
     */
    public final static int SDB_LOB_SEEK_END   = 2;
    
    /**
     * @fn          ObjectId getID()
     * @brief       get the lob's id
     * @return      the lob's id
     */
    public ObjectId getID();

    /**
     * @fn          long getSize()
     * @brief       get the size of lob
     * @return      the lob's size
     */
    public long getSize();
    
    /**
     * @fn          long getCreateTime()
     * @brief       get the create time of lob
     * @return      the lob's create time
     */
    public long getCreateTime();
    
    /**
     * @fn          write( byte[] b )
     * @brief       Writes <code>b.length</code> bytes from the specified 
     *              byte array to this lob. 
     * @param       b   the data.
     * @exception   com.sequoiadb.exception.BaseException
     */
    public void write( byte[] b ) throws BaseException;
    
    /**
     * @fn          int read( byte[] b )
     * @brief       Reads up to b.length bytes of data from this lob into 
     *              an array of bytes. 
     * @param       b   the buffer into which the data is read.
     * @return      the total number of bytes read into the buffer, or-1 if 
     *              there is no more data because the end of the file has been 
     *              reached, or 0 if b.length is Zero.
     * @exception   com.sequoiadb.exception.BaseException
     */
    public int read( byte[] b ) throws BaseException;
    
    /**
     * @fn          seek( long size, int seekType )
     * @brief       change the read position of the lob. The new position is 
     *              obtained by adding size to the position specified by 
     *              seekType. If seekType is set to SDB_LOB_SEEK_SET, 
     *              SDB_LOB_SEEK_CUR, or SDB_LOB_SEEK_END, the offset is 
     *              relative to the start of the lob, the current position 
     *              of lob, or the end of lob.
     * @param       size the adding size.
     * @param       seekType  SDB_LOB_SEEK_SET/SDB_LOB_SEEK_CUR/SDB_LOB_SEEK_END
     * @exception   com.sequoiadb.exception.BaseException.
     */
    public void seek( long size, int seekType ) throws BaseException;
    
    /**
     * @fn          close()
     * @brief       close the lob
     * @exception   com.sequoiadb.exception.BaseException
     */
    public void close() throws BaseException;
}

class DBLobConcrete implements DBLob {
    public final static int SDB_LOB_CREATEONLY = 0x00000001;
    public final static int SDB_LOB_READ       = 0x00000004;
    
    private final static int SDB_LOB_MAX_DATA_LENGTH  = 1024 * 1024;
    
    private final static long SDB_LOB_DEFAULT_OFFSET  = -1;
    private final static int SDB_LOB_DEFAULT_SEQ      = 0;
    
    private DBCollection _cl;
    private ObjectId     _id;
    private int          _mode;
    private long         _size;
    private long         _createTime;
    private long         _readOffset = 0;
    private boolean      _endianConvert;
    private boolean      _isOpen = false;
    
    /* when first open/create DBLob, sequoiadb return the contextID for the
     * further reading/writing/close
    */
    private long _contextID ;
    
    /**
     * @fn          DBLob( DBCollection cl )
     * @brief       Constructor
     * @param       cl   The instance of DBCollection 
     * @exception   com.sequoiadb.exception.BaseException
     */
    public DBLobConcrete( DBCollection cl ) throws BaseException {
        if ( cl == null ) {
            throw new BaseException( "SDB_INVALIDARG", "cl is null" );
        }
        
        _cl            = cl;
        _endianConvert = cl.getSequoiadb().endianConvert;
    }
    
    /**
     * @fn          open()
     * @brief       create a lob, lob's id will auto generate in this function
     * @exception   com.sequoiadb.exception.BaseException.
     */
    public void open() {
        open( null, SDB_LOB_CREATEONLY );
    }

    /**
     * @fn          open( ObjectId id )
     * @brief       open an exist lob with id
     * @param       id   the lob's id
     * @exception   com.sequoiadb.exception.BaseException.
     */
    public void open( ObjectId id ) {
        open( id, SDB_LOB_READ );
    }
    
    /**
     * @fn          open( ObjectId id, int mode )
     * @brief       open an exist lob, or create a lob
     * @param       id   the lob's id
     * @param       mode available mode is SDB_LOB_CREATEONLY or SDB_LOB_READ.
     *              SDB_LOB_CREATEONLY 
     *                  create a new lob with given id, if id is null, it will 
     *                  be generated in this function;
     *              SDB_LOB_READ
     *                  read an exist lob
     * @exception   com.sequoiadb.exception.BaseException.
     */
    public void open( ObjectId id, int mode ) throws BaseException {
        if ( _isOpen ) {
            throw new BaseException( "SDB_INVALIDARG", "lob have opened:id="
                    + _id );
        }
        
        if ( SDB_LOB_CREATEONLY != mode && SDB_LOB_READ != mode ) {
            throw new BaseException( "SDB_INVALIDARG", "mode is unsupported:" 
                            + mode );
        }
        
        if ( SDB_LOB_READ == mode ) {
            if ( null == id ) {
                throw new BaseException( "SDB_INVALIDARG", "id must be specify"
                        + " in mode:" + mode );
            }
        }
        
        _id = id;
        if ( SDB_LOB_CREATEONLY == mode ) {
            if ( null == _id ) {
                _id = ObjectId.get();
            }
        }

        _mode       = mode;
        _readOffset = 0;

        _open();
        _isOpen = true;
    }
    
    private void _open() throws BaseException {
        BSONObject openLob = new BasicBSONObject();
        openLob.put( SequoiadbConstants.FIELD_COLLECTION, _cl.getFullName() );
        openLob.put( SequoiadbConstants.FIELD_NAME_LOB_OID, _id );
        openLob.put( SequoiadbConstants.FIELD_NAME_LOB_OPEN_MODE, _mode );
        
        byte[] request = generateOpenLobRequest( openLob );
        ByteBuffer res = sendRequest( request, request.length );
        
        SDBMessage resMessage = SDBMessageHelper.msgExtractLobOpenReply( res );
        displayResponse( resMessage );
        if ( resMessage.getOperationCode() != Operation.MSG_BS_LOB_OPEN_RES) {
            throw new BaseException("SDB_UNKNOWN_MESSAGE", 
                    resMessage.getOperationCode());
        }
        int flag = resMessage.getFlags();
        if ( 0 != flag ) {
            throw new BaseException( flag, openLob );
        }
        
        List<BSONObject> objList = resMessage.getObjectList();
        if ( objList.size() != 1 ) {
            throw new BaseException( "SDB_NET_BROKEN_MSG", 
                    "objList.size()="+objList.size() );
        }
        
        BSONObject obj =  objList.get(0);
        _size = (Long) obj.get( SequoiadbConstants.FIELD_NAME_LOB_SIZE );
        _createTime = (Long) obj.get( 
                                SequoiadbConstants.FIELD_NAME_LOB_CREATTIME );
        _contextID = resMessage.getContextIDList().get(0);
    }
    
    private ByteBuffer sendRequest( byte[] request, int length )
            throws BaseException {
        if ( request == null ) {
            throw new BaseException( "SDB_INVALIDARG", "request can't be null" );
        }
        
        _cl.getConnection().sendMessage( request, length );
        return _cl.getConnection().receiveMessage(_endianConvert);
    }
    
    /**
     * @fn          getID()
     * @brief       get the lob's id
     * @return      the lob's id
     */
    public ObjectId getID() {
        return _id;
    }

    /**
     * @fn          getSize()
     * @brief       get the size of lob
     * @return      the lob's size
     */
    public long getSize() {
        return _size;
    }
    
    /**
     * @fn          getCreateTime()
     * @brief       get the create time of lob
     * @return      the lob's create time
     */
    public long getCreateTime() {
        return _createTime;
    }
    
    /**
     * @fn          close()
     * @brief       close the lob
     * @exception   com.sequoiadb.exception.BaseException
     */
    public void close() throws BaseException {
        if ( !_isOpen ) {
            return;
        }
        
        byte[] request = generateCloseLobRequest();
        ByteBuffer res = sendRequest( request, request.length );
        
        SDBMessage resMessage = SDBMessageHelper.msgExtractReply( res );
        displayResponse( resMessage );
        if ( resMessage.getOperationCode() != Operation.MSG_BS_LOB_CLOSE_RES) {
            throw new BaseException("SDB_UNKNOWN_MESSAGE", 
                    resMessage.getOperationCode());
        }
        int flag = resMessage.getFlags();
        if ( 0 != flag ) {
            throw new BaseException( flag );
        }
        
        _isOpen = false;
    }
    
    /**
     * @fn          write( byte[] b )
     * @brief       Writes <code>b.length</code> bytes from the specified 
     *              byte array to this lob. 
     * @param       b   the data.
     * @exception   com.sequoiadb.exception.BaseException.
     */
    public void write( byte[] b ) throws BaseException {
        if ( !_isOpen ) {
            throw new BaseException( "SDB_LOB_NOT_OPEN", "lob is not open" );
        }

        if ( b == null ) {
            throw new BaseException( "SDB_INVALIDARG", "input is null" );
        }
        
        if ( b.length <= SDB_LOB_MAX_DATA_LENGTH ) {
            _write( b );
            return;
        }
        
        /* if b.length is more then SDB_LOB_MAX_DATA_LENGTH. we will split 
         * the data to pieces with length=SDB_LOB_MAX_DATA_LENGTH. 
         * besides, data copy is a must in this case.
         */
        int offset = 0;
        while ( b.length > offset ) {
            int leftLen  = b.length - offset;
            int writeLen = leftLen > SDB_LOB_MAX_DATA_LENGTH ? 
                                             SDB_LOB_MAX_DATA_LENGTH : leftLen;
            
            _write( Arrays.copyOfRange( b, offset, offset + writeLen ) );
            
            offset += writeLen;
        }
    }
    
    /**
     * @fn          read( byte[] b )
     * @brief       Reads up to <code>b.length</code> bytes of data from this 
     *              lob into an array of bytes. 
     * @param       b   the buffer into which the data is read.
     * @return      the total number of bytes read into the buffer, or
     *              <code>-1</code> if there is no more data because the end of
     *              the file has been reached, or <code>0<code> if 
     *              <code>b.length</code> is Zero.
     * @exception   com.sequoiadb.exception.BaseException.
     */
    public int read( byte[] b ) throws BaseException {
        if ( !_isOpen ) {
            throw new BaseException( "SDB_LOB_NOT_OPEN", "lob is not open" );
        }
        
        if ( b == null ) {
            throw new BaseException( "SDB_INVALIDARG", "b is null" );
        }
        
        if ( b.length == 0 ) {
            return 0;
        }

        return _read( b );
    }
    
    /**
     * @fn          seek( long size, int seekType )
     * @brief       change the read position of the lob. The new position is 
     *              obtained by adding <code>size</code> to the position 
     *              specified by <code>seekType</code>. If <code>seekType</code> 
     *              is set to SDB_LOB_SEEK_SET, SDB_LOB_SEEK_CUR, or SDB_LOB_SEEK_END, 
     *              the offset is relative to the start of the lob, the current 
     *              position of lob, or the end of lob.
     * @param       size the adding size.
     * @param       seekType  SDB_LOB_SEEK_SET/SDB_LOB_SEEK_CUR/SDB_LOB_SEEK_END
     * @exception   com.sequoiadb.exception.BaseException.
     */
    public void seek( long size, int seekType ) throws BaseException {
        if ( !_isOpen ) {
            throw new BaseException( "SDB_LOB_NOT_OPEN", "lob is not open" );
        }

        if ( _mode != SDB_LOB_READ ) {
            throw new BaseException( "SDB_INVALIDARG", "seek() is not supported"
                    + "in mode=" + _mode );
        }
        
        if ( SDB_LOB_SEEK_SET == seekType ) {
            if ( size < 0 || size > _size ) {
                throw new BaseException( "SDB_INVALIDARG", "out of bound" );
            }
            
            _readOffset = size;
        }
        else if ( SDB_LOB_SEEK_CUR == seekType ) {
            if ( ( _size < _readOffset + size ) 
                    || ( _readOffset + size < 0 ) ) {
                throw new BaseException( "SDB_INVALIDARG", "out of bound" );
            }
            
            _readOffset += size;
        }
        else if ( SDB_LOB_SEEK_END == seekType ) {
            if ( size < 0 || size > _size ) {
                throw new BaseException( "SDB_INVALIDARG", "out of bound" );
            }
            
            _readOffset = _size - size;
        }
        else {
            throw new BaseException( "SDB_INVALIDARG", "unreconigzed seekType:"
                    + seekType );
        }
    }
    
    private int _read( byte[] b ) {
        byte[] request = generateReadLobRequest( b.length );
        ByteBuffer res = sendRequest( request, request.length );
        
        SDBMessage resMessage = SDBMessageHelper.msgExtractLobReadReply( res );
        displayResponse( resMessage );
        if ( resMessage.getOperationCode() != Operation.MSG_BS_LOB_READ_RES) {
            throw new BaseException("SDB_UNKNOWN_MESSAGE", 
                    resMessage.getOperationCode());
        }
        int flag = resMessage.getFlags();
        
        if ( SequoiadbConstants.SDB_EOF == flag ) {
            return -1;
        }
        
        if ( 0 != flag ) {
            throw new BaseException( flag );
        }
        
        byte[] lobData = resMessage.getLobBuff();
        for ( int i = 0; i < lobData.length; i++ ) {
            b[i] = lobData[i];
        }
        
        _readOffset += lobData.length;
        return lobData.length;
    }

    private byte[] generateReadLobRequest( int length ) {
        int totalLen = SDBMessageHelper.MESSAGE_OPLOB_LENGTH 
                + SDBMessageHelper.MESSAGE_LOBTUPLE_LENGTH;
        
        ByteBuffer buff = ByteBuffer.allocate( 
                                SDBMessageHelper.MESSAGE_OPLOB_LENGTH
                                + SDBMessageHelper.MESSAGE_LOBTUPLE_LENGTH );
        if ( _endianConvert ) {
            buff.order( ByteOrder.LITTLE_ENDIAN );
        } else {
            buff.order( ByteOrder.BIG_ENDIAN );
        }
        
        SDBMessageHelper.addLobMsgHeader( buff, totalLen, 
                Operation.MSG_BS_LOB_READ_REQ.getOperationCode(), 
                SequoiadbConstants.ZERO_NODEID, 0 );
        
        SDBMessageHelper.addLobOpMsg( buff, SequoiadbConstants.DEFAULT_VERSION, 
                   SequoiadbConstants.DEFAULT_W, (short)0, 
                   SequoiadbConstants.DEFAULT_FLAGS, _contextID, 0 );
        
        addMsgTuple( buff, length, SDB_LOB_DEFAULT_SEQ,
                _readOffset );
        
        return buff.array();
    }

    private void _write( byte[] input ) throws BaseException {
        byte[] request = generateWriteLobRequest( input );
        ByteBuffer res = sendRequest( request, request.length );
        
        SDBMessage resMessage = SDBMessageHelper.msgExtractReply( res );
        displayResponse( resMessage );
        if ( resMessage.getOperationCode() != Operation.MSG_BS_LOB_WRITE_RES) {
            throw new BaseException("SDB_UNKNOWN_MESSAGE", 
                    resMessage.getOperationCode());
        }
        int flag = resMessage.getFlags();
        if ( 0 != flag ) {
            throw new BaseException( flag );
        }
        
        _size += input.length;
    }
    
    private byte[] generateWriteLobRequest( byte[] input ) {
        int totalLen = SDBMessageHelper.MESSAGE_OPLOB_LENGTH 
                + SDBMessageHelper.MESSAGE_LOBTUPLE_LENGTH 
                + Helper.roundToMultipleXLength( input.length, 4 );
        
        ByteBuffer buff = ByteBuffer.allocate( 
                                SDBMessageHelper.MESSAGE_OPLOB_LENGTH
                                + SDBMessageHelper.MESSAGE_LOBTUPLE_LENGTH );
        if ( _endianConvert ) {
            buff.order( ByteOrder.LITTLE_ENDIAN );
        } else {
            buff.order( ByteOrder.BIG_ENDIAN );
        }
        
        SDBMessageHelper.addLobMsgHeader( buff, totalLen, 
                Operation.MSG_BS_LOB_WRITE_REQ.getOperationCode(), 
                SequoiadbConstants.ZERO_NODEID, 0 );
        
        SDBMessageHelper.addLobOpMsg( buff, SequoiadbConstants.DEFAULT_VERSION, 
                   SequoiadbConstants.DEFAULT_W, (short)0, 
                   SequoiadbConstants.DEFAULT_FLAGS, _contextID, 0 );
        
        addMsgTuple( buff, input.length, SDB_LOB_DEFAULT_SEQ,
                SDB_LOB_DEFAULT_OFFSET );
        
        List<byte[]> buffList = new ArrayList<byte[]>();
        buffList.add( buff.array() );
        buffList.add( Helper.roundToMultipleX( input, 4 ) );
        
        return Helper.concatByteArray( buffList );
    }

    private void addMsgTuple( ByteBuffer buff, int length, int sequence,
            long offset ) {
        
        buff.putInt( length );
        buff.putInt( sequence );
        buff.putLong( offset );
    }

    private byte[] generateCloseLobRequest() {
        int totalLen = SDBMessageHelper.MESSAGE_OPLOB_LENGTH;
        
        ByteBuffer buff = ByteBuffer.allocate( 
                                SDBMessageHelper.MESSAGE_OPLOB_LENGTH );
        if ( _endianConvert ) {
            buff.order( ByteOrder.LITTLE_ENDIAN );
        } else {
            buff.order( ByteOrder.BIG_ENDIAN );
        }
        
        SDBMessageHelper.addLobMsgHeader( buff, totalLen, 
                Operation.MSG_BS_LOB_CLOSE_REQ.getOperationCode(), 
                SequoiadbConstants.ZERO_NODEID, 0 );
        
        SDBMessageHelper.addLobOpMsg( buff, SequoiadbConstants.DEFAULT_VERSION, 
                   SequoiadbConstants.DEFAULT_W, (short)0, 
                   SequoiadbConstants.DEFAULT_FLAGS, _contextID, 0 );
        
        return buff.array();
    }
    
    private byte[] generateOpenLobRequest( BSONObject openLob ) {
        
        byte bOpenLob[] = SDBMessageHelper.bsonObjectToByteArray( openLob );
        int totalLen = SDBMessageHelper.MESSAGE_OPLOB_LENGTH 
                        + Helper.roundToMultipleXLength( bOpenLob.length, 4 );
        
        if ( !_endianConvert ) {
            SDBMessageHelper.bsonEndianConvert( bOpenLob, 0, bOpenLob.length, 
                    true );
        }
        
        ByteBuffer buff = ByteBuffer.allocate( 
                                SDBMessageHelper.MESSAGE_OPLOB_LENGTH );
        if ( _endianConvert ) {
            buff.order( ByteOrder.LITTLE_ENDIAN );
        } else {
            buff.order( ByteOrder.BIG_ENDIAN );
        }
        
        SDBMessageHelper.addLobMsgHeader( buff, totalLen, 
                Operation.MSG_BS_LOB_OPEN_REQ.getOperationCode(), 
                SequoiadbConstants.ZERO_NODEID, 0 );
        
        SDBMessageHelper.addLobOpMsg( buff, SequoiadbConstants.DEFAULT_VERSION, 
                   SequoiadbConstants.DEFAULT_W, (short)0, 
                   SequoiadbConstants.DEFAULT_FLAGS, 
                   SequoiadbConstants.DEFAULT_CONTEXTID, bOpenLob.length );
        
        List<byte[]> buffList = new ArrayList<byte[]>();
        buffList.add( buff.array() );
        buffList.add( Helper.roundToMultipleX( bOpenLob, 4 ) );

        return Helper.concatByteArray( buffList );
    }
    
    private void displayResponse( SDBMessage resMessage ) {
    }
}
