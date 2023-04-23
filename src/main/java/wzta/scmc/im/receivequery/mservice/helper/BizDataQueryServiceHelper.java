package wzta.scmc.im.receivequery.mservice.helper;

import java.util.*;

import kd.bos.dataentity.entity.DynamicObject;
import kd.bos.dataentity.entity.DynamicObjectCollection;
import kd.bos.dataentity.metadata.dynamicobject.DynamicObjectType;
import kd.bos.dataentity.utils.ObjectUtils;
import kd.bos.entity.botp.runtime.BFRowLinkDownNode;
import kd.bos.entity.botp.runtime.BFRowLinkUpNode;
import kd.bos.entity.botp.runtime.TableDefine;
import kd.bos.entity.datamodel.IDataModel;
import kd.bos.entity.report.IReportListModel;
import kd.bos.exception.KDBizException;
import kd.bos.form.FormShowParameter;
import kd.bos.mvc.report.ReportView;
import kd.bos.orm.query.QFilter;
import kd.bos.report.ReportForm;
import kd.bos.report.ReportList;
import kd.bos.servicehelper.BusinessDataServiceHelper;
import kd.bos.servicehelper.QueryServiceHelper;
import kd.bos.servicehelper.basedata.BaseDataServiceHelper;
import kd.bos.servicehelper.botp.BFTrackerServiceHelper;
import kd.bos.servicehelper.botp.ConvertMetaServiceHelper;
import kd.bos.web.DispatchServiceHelper;
import wzta.scmc.im.receivequery.mservice.utils.*;

public class BizDataQueryServiceHelper {
    public static List<Map<String, Object>> query(String entityName, String selectFields, List<List<Object>> filters, String orderBys, int top) {
        QFilter qFilter = QFilterUtils.getQFilterWithCheck(qF -> filters.forEach(ftr -> {
            if (ftr.size() != 3) {
                throw new KDBizException("传入的过滤条件错误！" + ftr.toString());
            }
            qF.and(ftr.get(0).toString(), ftr.get(1).toString(), ftr.get(2));
        }));
        return query(entityName, selectFields, qFilter, orderBys, top);
    }

    public static List<Map<String, Object>> queryBySqlWhere(String entityName, String selectFields, String sqlWhere, String orderBys, Integer top) {
        return query(entityName, selectFields, QFilter.of(sqlWhere, new Object[0]), orderBys, top.intValue());
    }
    public static List<Map<String, Object>> query(String entityName, String selectFields, QFilter filter, String orderBys, int top) {
        DynamicObjectCollection query = QueryServiceHelper.query(entityName, selectFields, filter.toArray(), orderBys, top);
        return CommonUtils.serializer2List(query);
    }

    public static <list, dynamicObjects> List<Map<String, Object>> queryReportByFront(String formId, Map<String, Object> data, Map<String, Map<Integer, String>> fieldsOrderMap) {
        FrontForm frontForm = new FrontForm();
        ReportView reportView = frontForm.creteReportView(formId);
        FormShowParameter formShowParameter = reportView.getFormShowParameter();
        ReportForm reportForm = (ReportForm)reportView.getRootControl();
        String pageId = reportView.getPageId();
        String appId = reportView.getServiceAppId(pageId);
        IDataModel reportModel = reportView.getModel();
        ReportList reportList = reportView.getReportList();
        IReportListModel reportListModel = reportList.getReportModel();
        HashMap<String, Object> actionParams = new HashMap<String, Object>();
        reportForm.setAsynQuery(false);
        reportList.setAsynQuery(false);
        try {
            frontForm.updateDataModel(reportModel, data, fieldsOrderMap);
            reportModel.updateCache();

            //System.out.println(new Date());
            String search = "[{\"key\":\"reportfilterap\",\"methodName\":\"search\",\"args\":[],\"postData\":[{},[]]}]";
            String getPercent = "[{\"key\":\"progressbarap\",\"methodName\":\"getPercent\",\"args\":[],\"postData\":[{},[]]}]";
            String getVirtualData = "[{\"key\":\"reportlistap\",\"methodName\":\"getVirtualData\",\"args\":[0,2000],\"postData\":[{},[]]}]";
            Object o = DispatchServiceHelper.invokeBOSServiceByAppId(appId, "FormService", "batchInvokeAction", new Object[] { pageId, search, actionParams });
            Object p = DispatchServiceHelper.invokeBOSServiceByAppId(appId, "FormService", "batchInvokeAction", new Object[] { pageId, getPercent, actionParams });
            Object g = DispatchServiceHelper.invokeBOSServiceByAppId(appId, "FormService", "batchInvokeAction", new Object[] { pageId, getVirtualData, actionParams });
            //System.out.println(new Date());
            //DynamicObjectCollection dynamicObjects = new DynamicObjectCollection();
            List datalist =new ArrayList<>();

            for (int i = 1; i <= reportListModel.getRowCount(); i++) {
                Map wlmap = new HashMap();
                //dynamicObjects.add(reportListModel.getRowData(i));
                DynamicObject rep = reportListModel.getRowData(i);
                wlmap.put("linetype",rep.get("linetype"));//数据类型
                wlmap.put("material",rep.get("material.number"));//物料编码
                wlmap.put("materialname",rep.get("material.name.zh_CN"));//物料名称
                wlmap.put("materialtype",rep.get("material.materialtype"));//物料类型
                wlmap.put("modelnum",rep.get("material.modelnum.zh_CN"));//规格型号
                wlmap.put("auxpty",rep.get("auxpty"));//辅助属性
                wlmap.put("warehouse",rep.get("warehouse")==null?rep.get("outwarehouse.number"):rep.get("warehouse.number"));//入库仓库
                wlmap.put("warehousename",rep.get("warehouse")==null?rep.get("outwarehouse.name.zh_CN"):rep.get("warehouse.name.zh_CN"));//入库仓库
                wlmap.put("location",rep.get("location")==null?rep.get("outlocation.number"):rep.get("location.number"));//仓位
                wlmap.put("locationname",rep.get("location")==null?rep.get("outlocation.name.zh_CN"):rep.get("location.name.zh_CN"));//仓位名称
                wlmap.put("invstatus",rep.get("invstatus")==null?rep.get("outinvstatus.number"):rep.get("invstatus.number"));//库存状态
                wlmap.put("invstatusname",rep.get("invstatus")==null?rep.get("outinvstatus.name.zh_CN"):rep.get("invstatus.name.zh_CN"));//库存状态
                wlmap.put("invtype",rep.get("invtype")==null?rep.get("outinvtype.number"):rep.get("invtype.number"));//库存类型
                wlmap.put("invtypename",rep.get("invtype")==null?rep.get("outinvtype.name.zh_CN"):rep.get("invtype.name.zh_CN"));//库存类型
                wlmap.put("billtype",rep.get("billtype.number"));//单据类型
                wlmap.put("billtypename",rep.get("billtype.name.zh_CN"));//单据类型
                wlmap.put("billno",rep.get("billno"));//单据编号
                wlmap.put("unit",rep.get("unit.number"));//计量单位
                wlmap.put("unitname",rep.get("unit.name.zh_CN"));//计量单位
                wlmap.put("baseunit",rep.get("baseunit.number"));//基本单位
                wlmap.put("baseunitname",rep.get("baseunit.name.zh_CN"));//基本单位
                wlmap.put("qty_in",rep.get("qty_in"));//数量(收入)
                wlmap.put("qty_out",rep.get("qty_out"));//数量(发出)
                wlmap.put("baseqty_in",rep.get("baseqty_in"));//基本数量(收入)
                wlmap.put("baseqty_out",rep.get("baseqty_out"));//基本数量(发出)
                wlmap.put("lotnumber",rep.get("lotnumber"));//批号
                wlmap.put("tracknumber",rep.get("tracknumber.number"));//跟踪号
                wlmap.put("trackname",rep.get("tracknumber.name.zh_CN"));//跟踪号
                wlmap.put("configuredcode",rep.get("configuredcode.number"));//配置号
                wlmap.put("configuredcodename",rep.get("configuredcode.name.zh_CN"));//配置号
                wlmap.put("project",rep.get("project.number"));//项目号
                wlmap.put("projectname",rep.get("project.name.zh_CN"));//项目号
                wlmap.put("org",rep.get("org.number"));//库存组织
                wlmap.put("orgname",rep.get("org.name.zh_CN"));//库存组织
                wlmap.put("ownertype",rep.get("ownertype"));//货主类型
                wlmap.put("owner",rep.get("owner.number"));//货主
                wlmap.put("owername",rep.get("owner.name.zh_CN"));//货主
                wlmap.put("keepertype",rep.get("keepertype"));//保管者类型
                wlmap.put("keeper",rep.get("keeper.number"));//保管者
                wlmap.put("keepername",rep.get("keeper.name.zh_CN"));//保管者
                wlmap.put("auditdate",rep.get("auditdate"));//审核时间
                wlmap.put("auditor",rep.get("auditor.number"));//审核人
                wlmap.put("auditorname",rep.get("auditor.name.zh_CN"));//审核人
                wlmap.put("creator",rep.get("creator.number"));//创建人
                wlmap.put("creatorname",rep.get("creator.name.zh_CN"));//创建人
                wlmap.put("mainbillnumber",rep.get("mainbillnumber"));//核心单据编号
                wlmap.put("biztype",rep.get("biztype.number"));//业务类型
                wlmap.put("biztypename",rep.get("biztype.name.zh_CN"));//业务类型
                wlmap.put("invscheme",rep.get("invscheme.number"));//库存事务
                wlmap.put("invschemename",rep.get("invscheme.name.zh_CN"));//库存事务
                wlmap.put("bookdate",rep.get("bookdate"));//j记账日期
                wlmap.put("biztime",rep.get("biztime"));//业务日期
                datalist.add(wlmap);
            }
            //List WLSF = CommonUtils.serializer2List(wlmap);
            // System.out.println(new Date());
            return datalist;
        } finally {
            frontForm.exitView(reportView);
        }
    }

    public static DynamicObject getBaseDataWithEnsureOne(String entityNumber, Map<String, Object> filters) { return getBaseDataWithEnsureOne(EntityUtils.getDynamicObjectType(entityNumber), filters); }

    public static DynamicObject getBaseDataWithEnsureOne(DynamicObjectType dt, Map<String, Object> filtersMap, Long orgID) {
        QFilter qFilter = QFilterUtils.getQFiltersFromMap(filtersMap);
        String mainOrgFiledName = EntityUtils.getEntityMetaMainOrgFiledName(dt.getName());
        if (!ObjectUtils.isEmpty(mainOrgFiledName) && orgID != null) {
            QFilter orgF = BaseDataServiceHelper.getBaseDataFilter(dt.getName(), orgID);
            qFilter.and(orgF);
        }
        return getBaseDataWithEnsureOne(dt, qFilter);
    }

    public static DynamicObject getBaseDataWithEnsureOne(DynamicObjectType dt, Map<String, Object> filtersMap) {
        QFilter qFilter = QFilterUtils.getQFiltersFromMap(filtersMap);
        return getBaseDataWithEnsureOne(dt, qFilter);
    }

    public static DynamicObject getBaseDataWithEnsureOne(DynamicObjectType dt, QFilter qFilter) {
        Map<Object, DynamicObject> loadData = BusinessDataServiceHelper.loadFromCache(dt, qFilter.toArray());
        if (loadData.size() != 1) {
            StringBuilder sb = new StringBuilder(String.format("基础资料"+dt.getName()+"不唯一!根据条件查询到"+Integer.valueOf(loadData.size()+"条记录qFilters="+Arrays.asList(qFilter.toArray()).toString().replace("1 = 1 AND ","")+";" ));
            if (!ObjectUtils.isEmpty(EntityUtils.getEntityMetaMainOrgFiledName(dt.getName()))) {
                sb.append("权限组织为").append(EntityUtils.getEntityMetaMainOrgFiledName(dt.getName()));
            }
            throw new KDBizException(sb.toString());
        }
        return (DynamicObject)loadData.values().iterator().next();
    }

    public static Map<String, Object> loadBillsUp(String entityNumber, List<Object> billIds, boolean onlyDirtSource) {
        Long[] ids = CommonUtils.getLongArray(billIds);
        Map<Long, BFRowLinkUpNode> bfRowLinkUpNodeMap = BFTrackerServiceHelper.loadBillLinkUpNodes(entityNumber, ids, onlyDirtSource);
        return CommonUtils.serializer2Map(bfRowLinkUpNodeMap);
    }

    public static Map<String, Object> loadBillsDown(String entityNumber, List<Object> billIds, boolean onlyDirtTarget) {
        Long[] ids = CommonUtils.getLongArray(billIds);
        Map<Long, BFRowLinkDownNode> loadBillLinkDownNodeMap = BFTrackerServiceHelper.loadBillLinkDownNodes(entityNumber, ids, onlyDirtTarget);
        return CommonUtils.serializer2Map(loadBillLinkDownNodeMap);
    }

    public static List<Map<String, Object>> loadTableDefine(List<Object> tableIds) {
        ArrayList<TableDefine> tableDefines = new ArrayList<TableDefine>();
        tableIds.forEach(tableId -> tableDefines.add(ConvertMetaServiceHelper.loadTableDefine(Long.valueOf(tableId.toString()))));
        return CommonUtils.serializer2List(tableDefines);
    }
}

