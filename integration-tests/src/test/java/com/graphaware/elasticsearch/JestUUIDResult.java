/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.elasticsearch;

import io.searchbox.annotations.JestId;

/**
 *
 * @author ale
 */
public class JestUUIDResult
{
    @JestId
    private String documentId;
    
    public String getDocumentId() {
      return documentId;
    }
}