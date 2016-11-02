package edu.virginia.lib.aptrust.bag;

import edu.virginia.lib.aptrust.bags.APTrustBag;
import edu.virginia.lib.aptrust.bags.APTrustInfo;
import edu.virginia.lib.aptrust.bags.BagInfo;

import java.io.File;
import java.util.Collections;
import java.util.List;

/**
 * Created by md5wz on 11/2/16.
 */
public class Libra1ResubmissionBag extends APTrustBag {

    private static File libraDump;

    public Libra1ResubmissionBag(BagInfo bagInfo, APTrustInfo aptrustInfo, final File libraDump) {
        super(bagInfo, aptrustInfo);
        this.libraDump = libraDump;
    }

    @Override
    protected String getItemId() {
        return "libra";
    }

    @Override
    protected List<File> getPayloadFiles() throws Exception {
        return Collections.singletonList(libraDump);
    }

    @Override
    protected void freePayloadFile(File file) throws Exception {
        // do nothing
    }
}
