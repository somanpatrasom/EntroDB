package com.entrodb.transaction;

public enum LockType {
    SHARED,    // READ  — multiple transactions can hold simultaneously
    EXCLUSIVE  // WRITE — only one transaction at a time
}
