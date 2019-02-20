package com.sample.pagination;


import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AppTest
{

    @Test
    public void testPagination(){

       Pagination pagination=new Pagination();
       String token=null;
       List<String> documentist=new ArrayList();
       do{
           Map<String, Object> resultMap = pagination.getPaginationResults(2, token);
           List<String> documents = (List<String>) resultMap.get("documents");
           String  ct = (String)resultMap.get("token");
           if(CollectionUtils.isNotEmpty(documents)){
               documentist.addAll(documents);
           }
           if(StringUtils.isNotBlank(ct)){
               token=ct;
           }else{
               token=null;
           }

       }while(token!=null);

        Assert.assertEquals(8,documentist.size());

}
}
