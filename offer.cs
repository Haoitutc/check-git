using Dapper;
using MainApi.DataLayer.Entities;
using MainApi.SharedLibs;
using MainApi.SharedLibs.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion.Internal;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Linq;
using System.Net.WebSockets;
using System.Reflection.Metadata;
using System.Text.RegularExpressions;
using static Dapper.SqlMapper;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace MainApi.DataLayer.Repositories
{
    public class RpsOffer
    {
        private readonly string _conStr;
        private readonly ManagerDbContext _db;
        public RpsOffer(string connectionString, ManagerDbContext db)
        {
            _conStr = connectionString;
            _db = ManagerDbInitializer.InitDBContext();
            _db.ChangeTracker.AutoDetectChangesEnabled = false;
        }

        public RpsOffer()
        {
            _conStr = AppConfiguration.GetAppsetting("MainDBConn");
        }

        #region Offer
        public List<IdentityOffer> GetRootByPage(IdentityOffer filter, int currentPage, int pageSize)
        {
            //Common syntax           
            var sqlCmd = @"Offer_GetRootByPage";
            List<IdentityOffer> listData = null;

            //For paging 
            int offset = (currentPage - 1) * pageSize;

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@AgencyId", filter.AgencyId },
                {"@Keyword", filter.Keyword },
                {"@CreatedBy", filter.CreatedBy },
                {"@Status", filter.Status },
                {"@Offset", offset},
                {"@PageSize", pageSize},
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    using (var reader = MsSqlHelper.ExecuteReader(conn, CommandType.StoredProcedure, sqlCmd, parms))
                    {
                        listData = new List<IdentityOffer>();
                        while (reader.Read())
                        {
                            //var r = ExtractOfferData(reader);
                            var r = new IdentityOffer();
                            r.OfferRootId = Utils.ConvertToInt32(reader["OfferRootId"]);
                            r.Id = Utils.ConvertToInt32(reader["Id"]);

                            if (reader.HasColumn("TotalCount"))
                                r.TotalCount = Utils.ConvertToInt32(reader["TotalCount"]);

                            listData.Add(r);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return listData;
        }

        public IdentityOfferRoot GetRootById(int id)
        {
            IdentityOfferRoot info = null;
            var sqlCmd = @"Offer_GetRootById";

            try
            {
                info = _db.tbl_offer_root.FirstOrDefault(x => x.Id == id);
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return info;
        }

        public List<IdentityOffer> GetRootByPageAndProjectId(IdentityOffer filter, int currentPage, int pageSize)
        {
            //Common syntax           
            var sqlCmd = @"Offer_GetRootByPageAndProjectId";
            List<IdentityOffer> listData = null;

            //For paging 
            int offset = (currentPage - 1) * pageSize;

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@AgencyId", filter.AgencyId },
                {"@Keyword", filter.Keyword },
                {"@CreatedBy", filter.CreatedBy },
                {"@Status", filter.Status },
                {"@Offset", offset},
                {"@PageSize", pageSize},
                {"@ProjectId", filter.ProjectId},
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    using (var reader = MsSqlHelper.ExecuteReader(conn, CommandType.StoredProcedure, sqlCmd, parms))
                    {
                        listData = new List<IdentityOffer>();
                        while (reader.Read())
                        {
                            //var r = ExtractOfferData(reader);
                            var r = new IdentityOffer();
                            r.OfferRootId = Utils.ConvertToInt32(reader["OfferRootId"]);
                            r.Id = Utils.ConvertToInt32(reader["Id"]);

                            if (reader.HasColumn("TotalCount"))
                                r.TotalCount = Utils.ConvertToInt32(reader["TotalCount"]);

                            listData.Add(r);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return listData;
        }

        public List<IdentityOfferRoot> GetRootsByProject(int id)
        {
            List<IdentityOfferRoot> list = null;
            var sqlCmd = @"Offer_GetRootsByProject";

            try
            {
                list = _db.tbl_offer_root.Where(x => x.ProjectId == id).OrderByDescending(x => x.Id).ToList();
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return list;
        }

        public IdentityCustomerOfferRoots GetRootsByCustomer(int customerId, int projectType)
        {
            IdentityCustomerOfferRoots m = null;
            var sqlCmd = @"Offer_GetRootsByCustomer";

            try
            {
                var projects = _db.tbl_project.Where(x => x.CustomerId == customerId && x.Type == projectType && x.Status != -99).ToList();
                if (projects.HasData())
                {
                    m = new IdentityCustomerOfferRoots();
                    m.Projects = projects;
                    m.OfferRoots = new List<IdentityOfferRoot>();
                    foreach (var p in projects)
                    {
                        var root = _db.tbl_offer_root.Where(x => x.ProjectId == p.Id).FirstOrDefault();
                        if (root != null)
                        {
                            m.OfferRoots.Add(root);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return m;
        }

        public List<IdentityOffer> GetRootByProjectId(IdentityOffer filter)
        {
            //Common syntax           
            var sqlCmd = @"Offer_GetRootByProjectId";
            List<IdentityOffer> listData = null;

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@ProjectId", filter.ProjectId},
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    using (var reader = MsSqlHelper.ExecuteReader(conn, CommandType.StoredProcedure, sqlCmd, parms))
                    {
                        listData = new List<IdentityOffer>();
                        while (reader.Read())
                        {
                            //var r = ExtractOfferData(reader);
                            var r = new IdentityOffer();
                            r.OfferRootId = Utils.ConvertToInt32(reader["OfferRootId"]);
                            r.Id = Utils.ConvertToInt32(reader["Id"]);

                            if (reader.HasColumn("TotalCount"))
                                r.TotalCount = Utils.ConvertToInt32(reader["TotalCount"]);

                            listData.Add(r);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return listData;
        }

        public List<IdentityOffer> GetByPage(IdentityOffer filter, int currentPage, int pageSize)
        {
            //Common syntax           
            var sqlCmd = @"Offer_GetByPage";
            List<IdentityOffer> listData = null;

            //For paging 
            int offset = (currentPage - 1) * pageSize;

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@OfferRootId", filter.OfferRootId },
                {"@Keyword", filter.Keyword },
                {"@CreatedBy", filter.CreatedBy },
                {"@Status", filter.Status },
                {"@Offset", offset},
                {"@PageSize", pageSize},
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    using (var reader = MsSqlHelper.ExecuteReader(conn, CommandType.StoredProcedure, sqlCmd, parms))
                    {
                        listData = new List<IdentityOffer>();
                        while (reader.Read())
                        {
                            //var r = ExtractOfferData(reader);
                            var r = new IdentityOffer();
                            r.Id = Utils.ConvertToInt32(reader["Id"]);

                            if (reader.HasColumn("TotalCount"))
                                r.TotalCount = Utils.ConvertToInt32(reader["TotalCount"]);

                            listData.Add(r);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return listData;
        }

        public List<IdentityOffer> GetOffersByRoot(int offerRootId)
        {
            //Common syntax           
            var sqlCmd = @"Offer_GetOffersByRoot";
            List<IdentityOffer> listData = null;

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@OfferRootId", offerRootId }
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    using (var reader = MsSqlHelper.ExecuteReader(conn, CommandType.StoredProcedure, sqlCmd, parms))
                    {
                        listData = new List<IdentityOffer>();
                        while (reader.Read())
                        {
                            var r = new IdentityOffer();
                            r.Id = Utils.ConvertToInt32(reader["Id"]);

                            listData.Add(r);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return listData;
        }

        public List<IdentityOffer> GetVersionsByRoot(int id)
        {
            List<IdentityOffer> list = null;
            var sqlCmd = @"Offer_GetVersionsByRoot";

            try
            {
                list = _db.tbl_offer.Where(x => x.OfferRootId == id && x.Status != -99).OrderByDescending(x => x.IsFinal).ThenByDescending(x => x.Id).ToList();
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return list;
        }

        public List<IdentityOfferMaterial> GetOfferMaterialsByGroup(int groupId)
        {
            List<IdentityOfferMaterial> list = null;

            var sqlCmd = @"Offer_GetMaterialsByGroup";

            var parameters = new DynamicParameters();
            parameters.Add("Id", groupId);

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var reader = SqlMapper.QueryMultipleAsync(conn, sqlCmd, parameters, commandType: CommandType.StoredProcedure).Result;
                    if (!reader.IsConsumed)
                    {
                        list = reader.Read<IdentityOfferMaterial>().ToList();
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }
            return list;
        }

        public List<IdentityOfferMaterial> GetOfferMaterialsByGroupIdx(int offerId, int groupIdx)
        {
            List<IdentityOfferMaterial> list = null;

            var sqlCmd = @"Offer_GetMaterialsByGroupIdx";

            var parameters = new DynamicParameters();
            parameters.Add("OfferId", offerId);
            parameters.Add("GroupIdx", groupIdx);

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var reader = SqlMapper.QueryMultipleAsync(conn, sqlCmd, parameters, commandType: CommandType.StoredProcedure).Result;
                    if (!reader.IsConsumed)
                    {
                        list = reader.Read<IdentityOfferMaterial>().ToList();
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }
            return list;
        }

        public List<IdentityOfferMaterial> GetOfferMaterialsBySubsite(int id)
        {
            List<IdentityOfferMaterial> list = null;

            var sqlCmd = @"Offer_GetMaterialsBySubsite";

            var parameters = new DynamicParameters();
            parameters.Add("Id", id);

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var reader = SqlMapper.QueryMultipleAsync(conn, sqlCmd, parameters, commandType: CommandType.StoredProcedure).Result;
                    if (!reader.IsConsumed)
                    {
                        list = reader.Read<IdentityOfferMaterial>().ToList();
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return list;
        }

        public List<IdentityOffer> GetSuggestion(IdentityOffer filter, int currentPage, int pageSize)
        {
            //Common syntax           
            var sqlCmd = @"Offer_GetSuggestion";
            List<IdentityOffer> listData = null;

            //For paging 
            int offset = (currentPage - 1) * pageSize;

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@Keyword", filter.Keyword },
                {"@ExceptIds", null },
                {"@Offset", offset},
                {"@PageSize", pageSize},
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    using (var reader = MsSqlHelper.ExecuteReader(conn, CommandType.StoredProcedure, sqlCmd, parms))
                    {
                        listData = new List<IdentityOffer>();
                        while (reader.Read())
                        {
                            var record = new IdentityOffer();
                            record.Id = Utils.ConvertToInt32(reader["Id"]);

                            if (reader.HasColumn("TotalCount"))
                                record.TotalCount = Utils.ConvertToInt32(reader["TotalCount"]);

                            listData.Add(record);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return listData;
        }

        public List<IdentityOfferWorkGroup> GetGroupsBySubsite(int id)
        {
            List<IdentityOfferWorkGroup> list = null;

            var sqlCmd = @"Offer_GetGroupsBySubsite";

            var parameters = new DynamicParameters();
            parameters.Add("Id", id);

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var reader = SqlMapper.QueryMultipleAsync(conn, sqlCmd, parameters, commandType: CommandType.StoredProcedure).Result;
                    if (!reader.IsConsumed)
                    {
                        list = reader.Read<IdentityOfferWorkGroup>().ToList();
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return list;
        }

        public static IdentityOfferCompare ExtractOfferCompareData(IDataReader reader)
        {
            var record = new IdentityOfferCompare();

            //Seperate properties
            record.Id = Utils.ConvertToInt32(reader["Id"]);
            record.OfferRootId = Utils.ConvertToInt32(reader["OfferRootId"]);
            record.OfferId = Utils.ConvertToInt32(reader["OfferId"]);
            record.FileName = reader["FileName"].ToString();
            record.FileSize = Utils.ConvertToInt32(reader["FileSize"]);
            record.FilePath = reader["FilePath"].ToString();
            record.RawContent = reader["RawContent"].ToString();
            record.GrammarCheckResults = reader["GrammarCheckResults"].ToString();
            record.Passed = Utils.ConvertToBoolean(reader["Passed"]);
            record.CreatedBy = Utils.ConvertToInt32(reader["CreatedBy"]);
            record.CreatedDate = reader["CreatedDate"] == DBNull.Value ? null : (DateTime?)reader["CreatedDate"];

            return record;
        }

        public static IdentityOfferLabel ExtractOfferLabelData(IDataReader reader)
        {
            var record = new IdentityOfferLabel();

            //Seperate properties
            record.Id = Utils.ConvertToInt32(reader["Id"]);
            record.OfferRootId = Utils.ConvertToInt32(reader["OfferRootId"]);
            record.OfferId = Utils.ConvertToInt32(reader["OfferId"]);

            var labels = reader["Labels"].ToString();
            var otherLabels = reader["OtherLabels"].ToString();

            record.Labels = JsonConvert.DeserializeObject<List<IdentityOfferLabelItem>>(labels);
            record.OtherLabels = JsonConvert.DeserializeObject<List<IdentityOfferLabelItem>>(otherLabels);

            return record;
        }

        private IdentityOffer ExtractOfferData(GridReader reader)
        {
            IdentityOffer info = null;

            if (!reader.IsConsumed)
            {
                info = reader.Read<IdentityOffer>().FirstOrDefault();

                if (info != null)
                {
                    info.Code = info.RootCode;

                    if (!reader.IsConsumed)
                    {
                        info.Groups = reader.Read<IdentityOfferWorkGroup>().ToList();
                        if (!reader.IsConsumed)
                        {
                            info.Configs = reader.Read<IdentityWorkSheetConfig>().ToList();
                        }
                    }
                }
            }

            return info;
        }

        public IdentityOffer GetById(int id)
        {
            IdentityOffer info = null;

            var sqlCmd = @"Offer_GetById";

            var parameters = new DynamicParameters();
            parameters.Add("Id", id);

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var reader = SqlMapper.QueryMultipleAsync(conn, sqlCmd, parameters, commandType: CommandType.StoredProcedure).Result;
                    info = ExtractOfferData(reader);
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return info;
        }

        public bool Delete(int id, int staffId)
        {
            //Common syntax            
            var sqlCmd = @"Offer_Delete";

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@Id", id}
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    MsSqlHelper.ExecuteNonQuery(conn, CommandType.StoredProcedure, sqlCmd, parms);
                }

                // activity log
                var oDelete = new IdentityOfferLog();
                oDelete.StaffId = staffId;
                oDelete.OfferId = id;
                oDelete.CreatedDate = DateTime.UtcNow;
                oDelete.Type = (int)EnumLogType.Delete;

                _db.ChangeTracker.Clear();
                _db.tbl_offer_log.Add(oDelete);
                _db.SaveChanges();
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public bool DeleteVersion(int id, int staffId)
        {
            //Common syntax            
            var sqlCmd = @"Offer_DeleteVersion";

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@Id", id}
            };

            try
            {
                var currentOffer = _db.tbl_offer.FirstOrDefault(x => x.Id == id && x.Status != -99);
                if (currentOffer != null)
                {
                    currentOffer.Status = -99;
                    _db.ChangeTracker.Clear();
                    _db.tbl_offer.Update(currentOffer);
                    _db.SaveChanges();

                    // activity log
                    var oDelete = new IdentityOfferLog();
                    oDelete.StaffId = staffId;
                    oDelete.OfferId = id;
                    oDelete.CreatedDate = DateTime.UtcNow;
                    oDelete.Type = (int)EnumLogType.DeleteVersion;

                    _db.ChangeTracker.Clear();
                    _db.tbl_offer_log.Add(oDelete);
                    _db.SaveChanges();
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public IdentityOffer GetDetail(int id)
        {
            IdentityOffer info = null;
            var sqlCmd = @"Offer_GetDetail";

            var parameters = new DynamicParameters();
            parameters.Add("Id", id);

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var reader = SqlMapper.QueryMultipleAsync(conn, sqlCmd, parameters, commandType: CommandType.StoredProcedure).Result;
                    info = ExtractOfferData(reader);
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }
            return info;
        }

        public int Insert(IdentityOffer identity)
        {
            //Common syntax           
            var sqlCmd = @"Offer_Insert";
            var newId = 0;
            var offerRootId = identity.OfferRootId;
            var versionNum = identity.VersionNum;

            var isNewVer = false;

            try
            {
                _db.ChangeTracker.Clear();

                if (offerRootId <= 0)
                {
                    var offerRoot = new IdentityOfferRoot();
                    offerRoot.AgencyId = identity.AgencyId;
                    offerRoot.CustomerId = identity.CustomerId;
                    offerRoot.ProjectId = identity.ProjectId;
                    offerRoot.CreatedBy = identity.CreatedBy;
                    offerRoot.Name = identity.Name;
                    offerRoot.InChargeBy = identity.InChargeBy;
                    offerRoot.Code = identity.Code;
                    offerRoot.CreatedDate = DateTime.UtcNow;
                    offerRoot.CreatedBy = identity.CreatedBy;

                    _db.tbl_offer_root.Add(offerRoot);
                    _db.SaveChanges();

                    offerRootId = offerRoot.Id;
                    versionNum = 1;
                }
                else
                {
                    isNewVer = true;
                }

                if (offerRootId > 0)
                {
                    identity.OfferRootId = offerRootId;
                    identity.CreatedDate = DateTime.UtcNow;

                    if (string.IsNullOrEmpty(identity.VersionName))
                    {
                        identity.VersionName = "バージョン " + versionNum;
                        identity.VersionNum = versionNum;
                    }

                    _db.tbl_offer.Add(identity);
                    _db.SaveChanges();

                    if (identity.Id > 0)
                    {
                        newId = isNewVer ? identity.Id : offerRootId;

                        if (identity.Groups.HasData())
                        {
                            foreach (var item in identity.Groups)
                            {
                                item.OfferId = identity.Id;
                                item.OfferRootId = offerRootId;
                                item.ProjectId = identity.ProjectId;

                                if (isNewVer)
                                {
                                    item.Id = 0;
                                }
                            }

                            _db.tbl_offer_group.AddRange(identity.Groups);
                            _db.SaveChanges();
                        }

                        if (identity.DetailLines.HasData())
                        {
                            foreach (var item in identity.DetailLines)
                            {
                                item.OfferId = identity.Id;
                                item.OfferRootId = offerRootId;
                                item.ProjectId = identity.ProjectId;

                                if (string.IsNullOrEmpty(item.SubMaterialStr) && item.SubMaterials.HasData())
                                {
                                    item.SubMaterialStr = JsonConvert.SerializeObject(item.SubMaterials);
                                }

                                if (isNewVer)
                                {
                                    item.Id = 0;
                                }
                            }

                            _db.tbl_offer_material.AddRange(identity.DetailLines);
                            _db.SaveChanges();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return newId;
        }

        public int RenetClone(IdentityOfferClone identity)
        {
            int newId = 0;
            //Common syntax           
            var sqlCmd = @"Offer_RenetClone";
            var parms = new DynamicParameters();
            parms.Add("OfferId", identity.OfferId);
            parms.Add("ProjectId", identity.ProjectId);
            parms.Add("CurrentUserId", identity.CurrentUserId);

            try
            {

                _db.ChangeTracker.Clear();

                var offer = _db.tbl_offer.FirstOrDefault(x => x.Id == identity.OfferId && x.Status != -99);
                if (offer != null)
                {
                    offer.Code = identity.Code;
                    offer.ProjectId = identity.ProjectId;
                    offer.CustomerId = identity.CustomerId;
                    offer.CreatedBy = identity.CurrentUserId;
                    offer.CreatedDate = DateTime.UtcNow;
                    offer.LastUpdatedBy = identity.CurrentUserId;
                    offer.LastUpdated = DateTime.UtcNow;

                    //new project đã có root offer thì sẽ thêm vào làm version mới
                    // còn không thig tạo mới root
                    IdentityOfferRoot offerRoot = null;
                    var createNewVer = false;
                    var renetOfferRoot = _db.tbl_offer_root.FirstOrDefault(x => x.Id == offer.OfferRootId && x.ProjectId == identity.ProjectId && x.Status != -99);
                    if (renetOfferRoot != null)
                    {
                        offerRoot = renetOfferRoot;
                        createNewVer = true;
                    }
                    else
                    {
                        var currentOfferRoot = _db.tbl_offer_root.FirstOrDefault(x => x.Id == offer.OfferRootId && x.Status != -99);
                        offerRoot = currentOfferRoot;
                    }

                    var offerGroups = _db.tbl_offer_group.Where(x => x.OfferId == identity.OfferId && x.Status != -99).ToList();
                    if (offerRoot != null)
                    {
                        //đánh lại version number
                        if (offerRoot.Id > 0 && createNewVer)
                        {
                            var groupOffers = _db.tbl_offer.Where(x => x.OfferRootId == offerRoot.Id && x.Status != -99).ToList();
                            offer.VersionNum = groupOffers.HasData() ? groupOffers.Count + 1 : 1;
                        }
                        else
                        {
                            offer.VersionNum = 1;
                            offerRoot.Id = 0;
                            offerRoot.Code = identity.Code;
                            offerRoot.ProjectId = identity.ProjectId;
                            offerRoot.CustomerId = identity.CustomerId;
                            offerRoot.CreatedBy = identity.CurrentUserId;
                            offerRoot.CreatedDate = DateTime.UtcNow;
                            offerRoot.LastUpdatedBy = identity.CurrentUserId;
                            offerRoot.LastUpdated = DateTime.UtcNow;

                            // save new root
                            _db.tbl_offer_root.Update(offerRoot);
                            _db.SaveChanges();
                            _db.ChangeTracker.Clear();
                        }

                        //save new offer
                        offer.Id = 0;
                        offer.OfferRootId = offerRoot.Id;
                        _db.tbl_offer.Add(offer);
                        _db.SaveChanges();
                        _db.ChangeTracker.Clear();
                        newId = offer.Id;

                        if (offerGroups.HasData())
                        {
                            foreach (var group in offerGroups)
                            {
                                var grpMaterial = _db.tbl_offer_material.Where(x => x.OfferId == identity.OfferId && x.GroupId == group.Id).ToList();

                                //save group as new 
                                group.Id = 0;
                                group.OfferId = offer.Id;
                                group.OfferRootId = offerRoot.Id;
                                group.ProjectId = identity.ProjectId;

                                _db.tbl_offer_group.Update(group);
                                _db.SaveChanges();
                                _db.ChangeTracker.Clear();

                                if (grpMaterial.HasData())
                                {
                                    foreach (var mt in grpMaterial)
                                    {
                                        mt.Id = 0;
                                        mt.OfferRootId = offerRoot.Id;
                                        mt.OfferId = offer.Id;
                                        mt.GroupId = group.Id;
                                        mt.ProjectId = identity.ProjectId;

                                        _db.tbl_offer_material.Update(mt);
                                        _db.SaveChanges();
                                        _db.ChangeTracker.Clear();
                                    }
                                }

                            }
                        }

                        //material not belong to any group
                        var materialsWithoutGrp = _db.tbl_offer_material.Where(x => x.OfferId == identity.OfferId && x.GroupId <= 0).ToList();
                        if (materialsWithoutGrp.HasData())
                        {
                            foreach (var mt in materialsWithoutGrp)
                            {
                                mt.Id = 0;
                                mt.OfferRootId = offerRoot.Id;
                                mt.OfferId = offer.Id;

                                _db.tbl_offer_material.Update(mt);
                                _db.SaveChanges();
                            }
                        }

                        _db.ChangeTracker.Clear();
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return newId;
        }

        public int InsertNewVersion(IdentityOffer identity)
        {
            //Common syntax           
            var sqlCmd = @"Offer_InsertNewVer";
            var newId = 0;

            try
            {
                if (identity.Id == 0)
                {
                    //var currentFinalVer = _db.tbl_offer.Where(x => x.IsFinal == true).FirstOrDefault();

                    //_db.ChangeTracker.Clear();

                    //if (currentFinalVer != null)
                    //{
                    //    currentFinalVer.IsFinal = false;
                    //    _db.tbl_offer.Update(currentFinalVer);
                    //}

                    //insert new offer
                    _db.tbl_offer.Add(identity);
                    _db.SaveChanges();

                    if (identity.Id > 0)
                    {
                        try
                        {
                            //insert material that belongs to no group
                            if (identity.DetailLines.HasData())
                            {
                                foreach (var item in identity.DetailLines)
                                {
                                    item.Id = 0;
                                    item.OfferId = identity.Id;
                                }

                                _db.tbl_offer_material.AddRange(identity.DetailLines);
                                _db.SaveChanges();
                            }
                        }
                        catch (Exception ex)
                        {
                            var strError = string.Format("InsertNewVersion: Failed to add material Error: {0}. Data {1}", (ex.InnerException != null ? ex.InnerException.Message : ex.Message), JsonConvert.SerializeObject(identity));
                            throw new CustomSQLException(strError);
                        }

                        try
                        {
                            //insert new groups and their materials
                            if (identity.Groups.HasData())
                            {
                                foreach (var group in identity.Groups)
                                {
                                    group.OfferId = identity.Id;
                                    group.Id = 0;

                                    _db.tbl_offer_group.Add(group);
                                    _db.SaveChanges();

                                    if (group.Id > 0)
                                    {
                                        if (group.Materials.HasData())
                                        {
                                            foreach (var item in group.Materials)
                                            {
                                                item.Id = 0;
                                                item.OfferId = identity.Id;
                                                item.GroupId = group.Id;
                                            }

                                            _db.tbl_offer_material.AddRange(group.Materials);
                                            _db.SaveChanges();
                                        }
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            var strError = string.Format("InsertNewVersion: Failed to add group Error: {0}. Data {1}", (ex.InnerException != null ? ex.InnerException.Message : ex.Message), JsonConvert.SerializeObject(identity));
                            throw new CustomSQLException(strError);
                        }
                    }
                }

                newId = identity.Id;
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return newId;
        }

        public int UpdateOfferMaterial(IdentityOfferMaterial identity)
        {
            var res = 0;
            var sqlCmd = @"UpdateOfferMaterial";

            try
            {
                _db.tbl_offer_material.Update(identity);
                _db.SaveChanges();
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return identity.Id;
        }

        //public bool Update(IdentityOffer identity)
        //{
        //    List<int> inputMtIds = new List<int>();
        //    List<int> inputGrpIds = new List<int>();

        //    var sqlCmd = @"Offer_Update";

        //    var parms = new Dictionary<string, object>
        //    {
        //        {"@Id", identity.Id},
        //        {"@OfferRootId", identity.OfferRootId},
        //        {"@VersionName", identity.VersionName },
        //        {"@Code", identity.Code },
        //        {"@Name", identity.Name},
        //        {"@OfferDate", identity.OfferDate },
        //        {"@DisplayName", identity.DisplayName},
        //        {"@CustomerId", identity.CustomerId },
        //        {"@CustomerName", identity.CustomerName },
        //        {"@InChargeBy", identity.InChargeBy },
        //        {"@ExpiryTime", identity.ExpiryTime },
        //        {"@SiteName", identity.SiteName },
        //        {"@DeliverySite", identity.DeliverySite },
        //        {"@DeliveryTime", identity.DeliveryTime },
        //        {"@PaymentTerm", identity.PaymentTerm },
        //        {"@Remark", identity.Remark },
        //        {"@AgencyId", identity.AgencyId},
        //        {"@TotalPrice", identity.TotalPrice},
        //        {"@OfferPrice", identity.OfferPrice},
        //        {"@CostPrice", identity.CostPrice},
        //        {"@GrossProfit", identity.GrossProfit},
        //        {"@GrossProfitRatio", identity.GrossProfitRatio},
        //        {"@Conditions", identity.Conditions },
        //        {"@StaffDisplay", identity.StaffDisplay },
        //        {"@ManagerDisplay", identity.ManagerDisplay },
        //        {"@ConsumeTaxDisplay1", identity.ConsumeTaxDisplay1 },
        //        {"@ConsumeTaxDisplay2", identity.ConsumeTaxDisplay2 },
        //        {"@CellConfigStr", identity.CellConfigStr }
        //    };

        //    try
        //    {
        //        SqlConnection conn = new SqlConnection(_conStr);
        //        conn.Open();

        //        SqlTransaction trans = conn.BeginTransaction("Offer_Update");

        //        try
        //        {
        //            //update tbl_offer
        //            MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, sqlCmd, parms).ExecuteNonQuery();

        //            //if no root, creat root
        //            if (identity.OfferRootId <= 0)
        //            {
        //                var rootParms = new Dictionary<string, object>
        //                {
        //                    {"@AgencyId", identity.AgencyId},
        //                    {"@CustomerId", identity.CustomerId},
        //                    {"@CreatedBy", identity.CreatedBy},
        //                };

        //                var rootObj = MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, "Offer_CreateRoot", rootParms).ExecuteScalar();

        //                identity.OfferRootId = Utils.ConvertToInt32(rootObj);
        //            }

        //            if (identity.OfferRootId <= 0)
        //            {
        //                var strError = string.Format("Failed to execute {0}.", sqlCmd);
        //                throw new CustomSQLException(strError);
        //            }
        //            else
        //            {
        //                var rootParms = new Dictionary<string, object>
        //                {
        //                    {"@Id", identity.OfferRootId},
        //                    {"@Name", identity.Name},
        //                    {"@InChargeBy", identity.InChargeBy}
        //                };

        //                MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, "Offer_UpdateRoot", rootParms).ExecuteNonQuery();
        //            }

        //            if (identity.Groups.HasData())
        //            {
        //                foreach (var group in identity.Groups)
        //                {
        //                    if (group.Id <= 0)
        //                    {
        //                        var gParams = new Dictionary<string, object>
        //                        {
        //                            {"@OfferRootId", identity.OfferRootId },
        //                            {"@OfferId", identity.Id },
        //                            {"@ProjectId", identity.ProjectId },
        //                            {"@SubsiteId", 0 },
        //                            {"@Name", group.Name },
        //                            {"@Specification", group.Specification },
        //                            {"@Quantity", group.Quantity },
        //                            {"@Unit", group.Unit },
        //                            {"@UnitPrice", group.UnitPrice },
        //                            {"@TotalPrice", group.TotalPrice },
        //                            {"@OfferPrice", group.OfferPrice },
        //                            {"@CostPrice", group.CostPrice },
        //                            {"@OfferRatio", group.OfferRatio },
        //                            {"@GrossProfit", group.GrossProfit },
        //                            {"@GrossProfitRatio", group.GrossProfitRatio },
        //                            {"@Note", group.Note },
        //                            {"@CellConfigStr", group.CellConfigStr },
        //                            {"@GroupIdx", group.GroupIdx }
        //                        };

        //                        MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, "Offer_InsertGroup", gParams).ExecuteNonQuery();
        //                    }
        //                    else
        //                    {
        //                        inputGrpIds.Add(group.Id);
        //                        var gParams = new Dictionary<string, object>
        //                        {
        //                            {"@Id", group.Id},
        //                            {"@Name", group.Name },
        //                            {"@Specification", group.Specification },
        //                            {"@Quantity", group.Quantity },
        //                            {"@Unit", group.Unit },
        //                            {"@UnitPrice", group.UnitPrice },
        //                            {"@TotalPrice", group.TotalPrice },
        //                            {"@OfferPrice", group.OfferPrice },
        //                            {"@CostPrice", group.CostPrice },
        //                            {"@OfferRatio", group.OfferRatio },
        //                            {"@GrossProfit", group.GrossProfit },
        //                            {"@GrossProfitRatio", group.GrossProfitRatio },
        //                            {"@Note", group.Note },
        //                            {"@CellConfigStr", group.CellConfigStr },
        //                            {"@GroupIdx", group.GroupIdx }
        //                        };

        //                        MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, "Offer_UpdateGroup", gParams).ExecuteScalar();
        //                    }
        //                }
        //            }

        //            if (identity.DetailLines.HasData() && identity.HasDetailUpdated)
        //            {
        //                foreach (var t in identity.DetailLines)
        //                {
        //                    if (t.Id > 0)
        //                    {
        //                        inputMtIds.Add(t.Id);

        //                        string subMtStr = t.SubMaterialStr;

        //                        if (string.IsNullOrEmpty(subMtStr) && t.SubMaterials.HasData())
        //                        {
        //                            subMtStr = JsonConvert.SerializeObject(t.SubMaterials, Formatting.Indented);
        //                        }

        //                        var tParams = new Dictionary<string, object>
        //                        {
        //                            {"@Id", t.Id },
        //                            {"@Idx", t.Idx },
        //                            {"@OfferRootId", identity.OfferRootId },
        //                            {"@OfferId", identity.Id },
        //                            {"@MaterialId", t.MaterialId },
        //                            {"@MaterialName", t.MaterialName },
        //                            {"@Brief", t.Brief },
        //                            {"@Detail", t.Detail },
        //                            {"@ConstructionItem", t.ConstructionItem },
        //                            {"@MakerId", t.MakerId },
        //                            {"@MakerIds", t.MakerIds },
        //                            {"@MakerName", t.MakerName },
        //                            {"@Quantity", t.Quantity },
        //                            {"@Unit", t.Unit },
        //                            {"@UnitId", t.UnitId },
        //                            {"@UnitPrice", t.UnitPrice },
        //                            {"@TotalPrice", t.TotalPrice },
        //                            {"@Note", t.Note },
        //                            {"@UnitCostPrice", t.UnitCostPrice },
        //                            {"@InstallPrice", t.InstallPrice },
        //                            {"@TotalCostPrice", t.TotalCostPrice },
        //                            {"@EmptyRow", t.EmptyRow },
        //                            {"@SubMaterialStr", subMtStr },
        //                            {"@CellConfigStr", t.CellConfigStr },
        //                            {"@ChildMaterialStr", t.ChildMaterialStr },
        //                            {"@GroupIdx", t.GroupIdx }
        //                        };

        //                        MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, "Offer_UpdateMaterial", tParams).ExecuteNonQuery();
        //                    }
        //                    else
        //                    {
        //                        string subMtStr = t.SubMaterialStr;

        //                        if (string.IsNullOrEmpty(subMtStr) && t.SubMaterials.HasData())
        //                        {
        //                            subMtStr = JsonConvert.SerializeObject(t.SubMaterials, Formatting.Indented);
        //                        }

        //                        var tParams = new Dictionary<string, object>
        //                        {
        //                            {"@Idx", t.Idx },
        //                            {"@OfferRootId", identity.OfferRootId },
        //                            {"@OfferId", identity.Id },
        //                            {"@MaterialId", t.MaterialId },
        //                            {"@SubsiteId", 0 },
        //                            {"@Brief", t.Brief },
        //                            {"@Detail", t.Detail },
        //                            {"@MaterialName", t.MaterialName },
        //                            {"@GroupId", 0 },
        //                            {"@ProjectId", identity.ProjectId },
        //                            {"@ConstructionItem", t.ConstructionItem },
        //                            {"@MakerId", t.MakerId },
        //                            {"@MakerIds", t.MakerIds },
        //                            {"@MakerName", t.MakerName },
        //                            {"@Quantity", t.Quantity },
        //                            {"@Unit", t.Unit },
        //                            {"@UnitId", t.UnitId },
        //                            {"@UnitPrice", t.UnitPrice },
        //                            {"@TotalPrice", t.TotalPrice },
        //                            {"@Note", t.Note },
        //                            {"@UnitCostPrice", t.UnitCostPrice },
        //                            {"@InstallPrice", t.InstallPrice },
        //                            {"@TotalCostPrice", t.TotalCostPrice },
        //                            {"@EmptyRow", t.EmptyRow },
        //                            {"@SubMaterialStr", subMtStr },
        //                            {"@CellConfigStr", t.CellConfigStr },
        //                            {"@ChildMaterialStr", t.ChildMaterialStr },
        //                            {"@GroupIdx", t.GroupIdx }
        //                        };

        //                        MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, "Offer_InsertMaterial", tParams).ExecuteNonQuery();
        //                    }
        //                }
        //            }

        //            //find & remove materials deleted by users
        //            if (!string.IsNullOrEmpty(identity.DetailLineIds))
        //            {
        //                List<int> listDtIds = JsonConvert.DeserializeObject<List<int>>(identity.DetailLineIds);

        //                if (listDtIds.HasData() && inputMtIds.HasData())
        //                {
        //                    List<int> itemsOnlyInOrig = listDtIds.Except(inputMtIds).ToList();

        //                    if (itemsOnlyInOrig.HasData())
        //                    {
        //                        foreach (var item in itemsOnlyInOrig)
        //                        {
        //                            var delMtParams = new Dictionary<string, object>
        //                            {
        //                                {"@Id", item}
        //                            };

        //                            MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, "Offer_DeleteMaterial", delMtParams).ExecuteNonQuery();
        //                        }
        //                    }
        //                }
        //            }

        //            //find & remove groups deleted by users
        //            if (!string.IsNullOrEmpty(identity.GroupIds))
        //            {
        //                List<int> listGrpIds = JsonConvert.DeserializeObject<List<int>>(identity.GroupIds);

        //                if (listGrpIds.HasData() && inputGrpIds.HasData())
        //                {
        //                    List<int> itemsOnlyInOrig = listGrpIds.Except(inputGrpIds).ToList();

        //                    if (itemsOnlyInOrig.HasData())
        //                    {
        //                        foreach (var item in itemsOnlyInOrig)
        //                        {
        //                            var delGrpParams = new Dictionary<string, object>
        //                            {
        //                                {"@Id", item}
        //                            };

        //                            MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, "Offer_DeleteGroup", delGrpParams).ExecuteNonQuery();
        //                        }
        //                    }
        //                }
        //            }

        //            trans.Commit();
        //        }
        //        catch (Exception tranEx)
        //        {
        //            trans.Rollback();

        //            throw tranEx;
        //        }

        //        finally
        //        {
        //            if (conn != null)
        //            {
        //                conn.Close();
        //                conn.Dispose();
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
        //        throw new CustomSQLException(strError);
        //    }

        //    return true;
        //}

        public bool UpdateStatus(IdentityOffer identity)
        {
            //Common syntax
            var sqlCmd = @"Offer_UpdateStatus";

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@Id", identity.Id},
                {"@Status", identity.Status },
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    MsSqlHelper.ExecuteScalar(conn, CommandType.StoredProcedure, sqlCmd, parms);
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public bool Update(IdentityOffer info) 
        {
            var sqlCmd = @"Offer_Update";
            string trackName = "update cost and profit";

            try
            {
                var currentOffer = _db.tbl_offer.Where(x => x.Id == info.Id).FirstOrDefault();
                if (currentOffer != null)
                {
                    currentOffer.TotalPrice = info.TotalPrice;
                    currentOffer.CostPrice = info.CostPrice;
                    currentOffer.GrossProfit = info.GrossProfit;
                    currentOffer.GrossProfitRatio = info.GrossProfitRatio;

                    _db.tbl_offer.Update(currentOffer);
                    _db.SaveChanges();
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0} during {1}. Error: {2}", sqlCmd, trackName, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public bool UpdateInquiryUpdated(IdentityOffer info)
        {
            var sqlCmd = @"Offer_Update";
            string trackName = "update InquiryUpdated";

            try
            {
                var currentOffer = _db.tbl_offer.Where(x => x.Id == info.Id).FirstOrDefault();
                if (currentOffer != null)
                {
                    currentOffer.IsInquiryUpdated = info.IsInquiryUpdated;

                    _db.tbl_offer.Update(currentOffer);
                    _db.SaveChanges();
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0} during {1}. Error: {2}", sqlCmd, trackName, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public bool UpdateJanCode(int id, int packIdx, string janCode)
        {
            //Common syntax
            var sqlCmd = @"Offer_UpdateJanCode";

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@Id", id},
                {"@JanCode", janCode },
                {"@PackIdx", packIdx },
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    MsSqlHelper.ExecuteScalar(conn, CommandType.StoredProcedure, sqlCmd, parms);
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public bool UpdateOfferPackJanCode(int id, string janCode)
        {
            //Common syntax
            var sqlCmd = @"Offer_UpdatePackJanCode";

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@Id", id},
                {"@JanCode", janCode },
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    MsSqlHelper.ExecuteScalar(conn, CommandType.StoredProcedure, sqlCmd, parms);
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public int JancodeFinalCount(string jancode)
        {
            //Common syntax           
            var sqlCmd = @"Offer_JancodeFinalCount";
            var count = 0;

            //For parameters
            var parameters = new Dictionary<string, object>
            {
                {"@JancodeFinal", jancode},
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var returnObj = MsSqlHelper.ExecuteScalar(conn, CommandType.StoredProcedure, sqlCmd, parameters);
                    count = Convert.ToInt32(returnObj);
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return count;
        }

        public bool SetIsFinal(int id)
        {
            //Common syntax
            var sqlCmd = @"Offer_SetIsFinal";

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@OfferId", id}
            };

            try
            {
                SqlConnection conn = new SqlConnection(_conStr);
                conn.Open();

                SqlTransaction trans = conn.BeginTransaction("Offer_SetIsFinal");

                try
                {
                    //Set is final
                    MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, sqlCmd, parms).ExecuteScalar();


                    var historyParms = new Dictionary<string, object>
                    {
                        {"@OfferId", id}
                    };

                    //Storage to history
                    MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, @"Offer_UpdateFinalHistory", historyParms).ExecuteScalar();

                    trans.Commit();
                }
                catch (Exception tranEx)
                {
                    trans.Rollback();

                    throw tranEx;
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public bool UnSetIsFinal(int id)
        {
            //Common syntax
            var sqlCmd = @"Offer_UnSetIsFinal";

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@OfferId", id}
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    MsSqlHelper.ExecuteScalar(conn, CommandType.StoredProcedure, sqlCmd, parms);
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public int GetNextVersionNum(int offerRootId)
        {
            //Common syntax
            var sqlCmd = @"Offer_GetMaxVersionNumForInsert";

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@OfferRootId", offerRootId}
            };

            var returnNum = 1;

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var returnObj = MsSqlHelper.ExecuteScalar(conn, CommandType.StoredProcedure, sqlCmd, parms);
                    returnNum = Utils.ConvertToInt32(returnObj, 1);
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return returnNum;
        }

        public bool SaveMakersForOffer(List<IdentityOfferMaterial> offerMaterials)
        {
            //Common syntax            
            var sqlCmd = @"Offer_SaveMakerForOfferMaterial";

            //For parms
            var parms = new Dictionary<string, object>();

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    if (offerMaterials.HasData())
                    {
                        foreach (var offerMaterial in offerMaterials)
                        {
                            parms = new Dictionary<string, object>();
                            parms.Add("@OfferMaterialId", offerMaterial.Id);
                            parms.Add("@MakerIds", offerMaterial.MakerIds);
                            parms.Add("@ChildMaterialStr", offerMaterial.ChildMaterialStr);
                            parms.Add("@UnitCostPrice", offerMaterial.UnitCostPrice);
                            parms.Add("@TotalCostPrice", offerMaterial.TotalCostPrice);
                            parms.Add("@TotalCostPriceWithoutSurcharge", offerMaterial.TotalCostPriceWithoutSurcharge);
                            parms.Add("@UnitCostPriceWithoutSurcharge", offerMaterial.UnitCostPriceWithoutSurcharge);
                            parms.Add("@Note", offerMaterial.Note);
                            parms.Add("@ChosenMakers", offerMaterial.ChosenMakers);
                            parms.Add("@UnitPrice", offerMaterial.UnitPrice);
                            parms.Add("@TotalPrice", offerMaterial.TotalPrice);

                            MsSqlHelper.ExecuteNonQuery(conn, CommandType.StoredProcedure, sqlCmd, parms);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public int SaveManualMakerForOffer(IdentityManualInquiryMaterial indentity)
        {
            //Common syntax            
            var sqlCmd = @"Offer_SaveManualMaker";
            var returnId = 0;
            //For parms
            var parms = new Dictionary<string, object>();

            try
            {
                _db.ChangeTracker.Clear();
                var currentInfo = _db.tbl_manual_inquiry_material.FirstOrDefault(x => x.Id == indentity.Id);

                if (currentInfo != null)
                {
                    _db.Entry(currentInfo).Property(x => x.CreatedBy).IsModified = false;
                    _db.Entry(currentInfo).Property(x => x.CreatedDate).IsModified = false;
                }

                _db.tbl_manual_inquiry_material.Update(indentity);
                _db.SaveChanges();

                returnId = indentity.Id;
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return returnId;
        }

        public List<IdentityManualInquiryMaterial> GetManualMakerForOffer(int offerId)
        {
            //Common syntax            
            var sqlCmd = @"Offer_GetManualMakerForOffer";
            //For parms
            List<IdentityManualInquiryMaterial> res = null;

            try
            {
                _db.ChangeTracker.Clear();
                res = _db.tbl_manual_inquiry_material.Where(x => x.OfferId == offerId && x.Status != -99).ToList();

            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return res;
        }

        public bool DeleteManualMakerForOffer(int id)
        {
            //Common syntax            
            var sqlCmd = @"Offer_DeleteManualMaker";
            //For parms
            var parms = new Dictionary<string, object>();

            try
            {
                _db.ChangeTracker.Clear();
                var currentInfo = _db.tbl_manual_inquiry_material.FirstOrDefault(x => x.Id == id);


                _db.tbl_manual_inquiry_material.Remove(currentInfo);
                _db.SaveChanges();
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public bool SaveVersionName(IdentityOffer offer)
        {
            //Common syntax            
            var sqlCmd = @"Offer_SaveVersionName";

            //For parms
            var parms = new Dictionary<string, object>();

            try
            {
                var cOffer = _db.tbl_offer.FirstOrDefault(x => x.Id == offer.Id);
                if (cOffer != null)
                {
                    cOffer.VersionName = offer.VersionName;
                    _db.tbl_offer.Update(cOffer);
                    _db.SaveChanges();
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public IdentityOfferSummary PatchOfferDataSummary(int id)
        {
            IdentityOfferSummary returnObj = null;
            var sqlCmd = @"Offer_UpdateDataSummary";
            string trackName = "calculate data summary";
            string[] sumKeyword = { "合計", "小計", "中計", "仕切", "値引" };

            try
            {
                var currentOffer = _db.tbl_offer.Where(x => x.Id == id).FirstOrDefault();
                if (currentOffer != null)
                {
                    decimal? totalPrice = currentOffer.TotalPrice;
                    decimal? offerPrice = currentOffer.OfferPrice;
                    decimal? costPrice = 0;
                    decimal? grossProfit;
                    decimal? grossProfitRatio = null;

                    var currentListMt = _db.tbl_offer_material.Where(x => x.OfferId == currentOffer.Id).ToList();

                    //calculate offer data summary (total cost)
                    if (currentListMt.HasData())
                    {
                        trackName = "calculating total cost";
                        foreach (var item in currentListMt)
                        {
                            if (!string.IsNullOrEmpty(item.ConstructionItem))
                            {
                                var wacthText = item.ConstructionItem.ToStringNormally().Replace(" ", "");
                                if (!sumKeyword.Contains(wacthText))
                                {
                                    costPrice += item.TotalCostPrice.GetValueOrDefault();
                                }
                            }                            
                        }
                    }

                    //calculate offer data summary (gross profit, gross profit ratio)
                    trackName = "calculating profit";

                    grossProfit = offerPrice.GetValueOrDefault() - costPrice.GetValueOrDefault();

                    if (costPrice.GetValueOrDefault() != 0)
                    {
                        grossProfitRatio = offerPrice.GetValueOrDefault() / costPrice.GetValueOrDefault();
                    }

                    //update offer with just calculated profit and cost
                    trackName = "udpate offer";
                    currentOffer.CostPrice = costPrice;
                    currentOffer.GrossProfit = grossProfit;
                    currentOffer.GrossProfitRatio = grossProfitRatio;

                    _db.tbl_offer.Update(currentOffer);
                    _db.SaveChanges();

                    returnObj = new IdentityOfferSummary
                    {
                        OfferId = id,
                        CostPrice = costPrice,
                        GrossProfit = grossProfit,
                        GrossProfitRatio = grossProfitRatio
                    };
                }          
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0} during {1}. Error: {2}. Item: {3}", sqlCmd, trackName, (ex.InnerException != null ? ex.InnerException.Message : ex.Message), id);
                returnObj = null;
                throw new CustomSQLException(strError);
            }

            return returnObj;
        }

        #endregion

        #region Choose Makers For Offer
        public List<IdentityOfferMaterial> GetOfferMaterialsByOffer(int offerId)
        {
            List<IdentityOfferMaterial> list = null;

            var sqlCmd = @"Offer_GetMaterialsByOffer";

            var parameters = new DynamicParameters();
            parameters.Add("Id", offerId);

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var reader = SqlMapper.QueryMultipleAsync(conn, sqlCmd, parameters, commandType: CommandType.StoredProcedure).Result;
                    if (!reader.IsConsumed)
                    {
                        list = reader.Read<IdentityOfferMaterial>().ToList();
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return list;
        }

        public List<IdentityOfferMaterial> GetOfferMaterialsByPage(int offerId, int currentPage, int pageSize)
        {
            List<IdentityOfferMaterial> list = null;

            var sqlCmd = @"Offer_GetMaterialsByPage";

            //For paging 
            int offset = (currentPage - 1) * pageSize;

            var parameters = new DynamicParameters();
            parameters.Add("OfferId", offerId);
            parameters.Add("Offset", offset);
            parameters.Add("PageSize", pageSize);

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var reader = SqlMapper.QueryMultipleAsync(conn, sqlCmd, parameters, commandType: CommandType.StoredProcedure).Result;
                    if (!reader.IsConsumed)
                    {
                        list = reader.Read<IdentityOfferMaterial>().ToList();
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return list;
        }

        public bool UpdateCostAndPrice(IdentityOffer info)
        {
            string sqlCmd = "Offer_UpdateCostAndPrice";
            string trackName = "update offer";
            string[] sumKeyword = { "合計", "中計", "小計", "仕切", "値引" };

            try
            {
                var currentOffer = _db.tbl_offer.Where(x => x.Id == info.Id).FirstOrDefault();
                if (currentOffer != null) 
                {
                    //update total price, cost price and profit
                    currentOffer.TotalPrice = info.TotalPrice;
                    currentOffer.CostPrice = info.CostPrice;

                    currentOffer.GrossProfit = currentOffer.OfferPrice.GetValueOrDefault() - currentOffer.CostPrice.GetValueOrDefault();
                    if (currentOffer.CostPrice.GetValueOrDefault() != 0)
                    {
                        currentOffer.GrossProfitRatio = currentOffer.OfferPrice.GetValueOrDefault() / currentOffer.CostPrice.GetValueOrDefault();
                    }

                    currentOffer.PriceDisplayType = info.PriceDisplayType;

                    _db.tbl_offer.Update(currentOffer);
                    _db.SaveChanges();
                    
                    //update materials
                    if (info.DetailLines.HasData())
                    {
                        trackName = "update materials";
                        _db.ChangeTracker.Clear();

                        _db.tbl_offer_material.UpdateRange(info.DetailLines);
                        _db.SaveChanges();
                    }

                    //update groups                  
                    var currentItems = _db.tbl_offer_material.Where(x => x.OfferId == currentOffer.Id).OrderBy(x => x.SortOrder).ToList();
                    if (currentItems.HasData())
                    {
                        trackName = "update item with sum keywords";
                        _db.ChangeTracker.Clear();

                        decimal tempTotalPrice = 0;
                        decimal tempInstallTotal = 0;
                        decimal tempCostPrice = 0;

                        foreach (var item in currentItems)
                        {
                            if (sumKeyword.Contains(item.ConstructionItem))
                            {
                                if (item.ConstructionItem == "合計")
                                {
                                    item.TotalPrice = info.TotalPrice;
                                    item.TotalCostPrice = info.CostPrice;
                                    item.InstallTotal = tempInstallTotal;

                                    tempTotalPrice = 0;
                                    tempInstallTotal = 0;
                                    tempCostPrice = 0;
                                }
                                else if (item.ConstructionItem == "仕切")
                                {
                                    item.TotalPrice = info.OfferPrice;
                                }
                                else if (item.ConstructionItem == "値引")
                                {
                                    var myTotalPrice = info.TotalPrice.GetValueOrDefault() - info.OfferPrice.GetValueOrDefault();
                                    if (myTotalPrice != 0)
                                    {
                                        item.TotalPrice = myTotalPrice;
                                    }
                                }
                                else if (item.ConstructionItem == "小計" || item.ConstructionItem == "中計")
                                {
                                    item.TotalPrice = tempTotalPrice;
                                    item.InstallTotal = tempInstallTotal;
                                    item.TotalCostPrice = tempCostPrice;

                                    tempTotalPrice = 0;
                                    tempInstallTotal = 0;
                                    tempCostPrice = 0;
                                }
                            }
                            else
                            {
                                if (item.Type != 1)
                                {
                                    tempTotalPrice += item.TotalPrice.GetValueOrDefault();
                                    tempInstallTotal += item.InstallTotal.GetValueOrDefault();
                                    tempCostPrice += item.TotalCostPrice.GetValueOrDefault();
                                }
                            }
                        }

                        _db.tbl_offer_material.UpdateRange(currentItems);
                        _db.SaveChanges();

                        if (info.Groups.HasData())
                        {
                            trackName = "update groups";
                            _db.ChangeTracker.Clear();
                            foreach (var group in info.Groups)
                            {
                                var itemsInGroup = currentItems.Where(x => x.GroupId == group.Id && x.Type != 1 && !sumKeyword.Contains(x.ConstructionItem)).ToList();

                                if (itemsInGroup.HasData())
                                {
                                    var currentGroup = _db.tbl_offer_group.Where(x => x.Id == group.Id).FirstOrDefault();
                                    if (currentGroup != null)
                                    {
                                        currentGroup.TotalPrice = itemsInGroup.Sum(x => x.TotalPrice);
                                        _db.tbl_offer_group.Update(currentGroup);
                                    }
                                }
                            }

                            _db.SaveChanges();
                        }                        
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0} during {1}. Error: {2}", sqlCmd, trackName, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        #endregion

        #region Label

        public IdentityOfferLabel GetLabelInfo(int offerId)
        {
            IdentityOfferLabel info = null;

            var sqlCmd = @"Offer_GetLabel";

            var parms = new Dictionary<string, object>
            {
                {"@OfferId", offerId}
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    using (var reader = MsSqlHelper.ExecuteReader(conn, CommandType.StoredProcedure, sqlCmd, parms))
                    {
                        if (reader.Read())
                        {
                            info = ExtractOfferLabelData(reader);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }
            return info;
        }

        public int UpdateLabel(IdentityOfferLabel identity)
        {
            var returnId = 0;
            //Common syntax
            var sqlCmd = @"Offer_UpdateLabel";
            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var delParams = new Dictionary<string, object>
                    {
                        {"@Id", identity.OfferId }
                    };

                    var labels = JsonConvert.SerializeObject(identity.Labels);
                    var otherLabels = JsonConvert.SerializeObject(identity.OtherLabels);

                    var lbParms = new Dictionary<string, object>
                    {
                        {"@OfferRootId", identity.OfferRootId },
                        {"@OfferId", identity.OfferId },
                        {"@Labels", labels },
                        {"@OtherLabels", otherLabels },
                        {"@CreatedBy", identity.CreatedBy }
                    };

                    //Update label
                    var returnObj = MsSqlHelper.ExecuteScalar(conn, CommandType.StoredProcedure, @"Offer_UpdateLabel", lbParms);

                    returnId = Utils.ConvertToInt32(returnObj);
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return returnId;
        }

        public int ComparisonWriteLog(IdentityOfferCompare identity)
        {
            var returnId = 0;
            //Common syntax
            var sqlCmd = @"Offer_ComparisonWriteLog";
            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var parms = new Dictionary<string, object>
                    {
                        {"@OfferId", identity.OfferId },
                        {"@OfferRootId", identity.OfferRootId },
                        {"@FileName", identity.FileName },
                        {"@FileSize", identity.FileSize },
                        {"@FilePath", identity.FilePath },
                        {"@RawContent", identity.RawContent },
                        {"@GrammarCheckResults", identity.GrammarCheckResults },
                        {"@Passed", identity.Passed },
                        {"@CreatedBy", identity.CreatedBy }
                    };

                    var returnObj = MsSqlHelper.ExecuteScalar(conn, CommandType.StoredProcedure, sqlCmd, parms);

                    returnId = Utils.ConvertToInt32(returnObj);
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return returnId;
        }

        public bool ClearLabel(int offerId)
        {
            //Common syntax            
            var sqlCmd = @"Offer_ClearLabel";

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@Id", offerId}
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    MsSqlHelper.ExecuteNonQuery(conn, CommandType.StoredProcedure, sqlCmd, parms);
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public List<IdentityOfferCompare> ComparisonHistory(IdentityOfferCompare filter)
        {
            //Common syntax           
            var sqlCmd = @"Offer_ComparisonHistory";
            List<IdentityOfferCompare> listData = null;

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@OfferId", filter.OfferId }
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    using (var reader = MsSqlHelper.ExecuteReader(conn, CommandType.StoredProcedure, sqlCmd, parms))
                    {
                        listData = new List<IdentityOfferCompare>();
                        while (reader.Read())
                        {
                            var r = ExtractOfferCompareData(reader);
                            listData.Add(r);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return listData;
        }

        public IdentityOfferCompare ComparisonDetail(int id)
        {
            //Common syntax           
            var sqlCmd = @"Offer_ComparisonDetail";
            IdentityOfferCompare info = null;

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@Id", id }
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    using (var reader = MsSqlHelper.ExecuteReader(conn, CommandType.StoredProcedure, sqlCmd, parms))
                    {
                        if (reader.Read())
                        {
                            info = ExtractOfferCompareData(reader);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return info;
        }

        public List<IdentityOffer> GetRootByCustomer(IdentityOffer filter, int currentPage, int pageSize)
        {
            //Common syntax           
            var sqlCmd = @"Offer_GetRootByCustomer";
            List<IdentityOffer> listData = null;

            //For paging 
            int offset = (currentPage - 1) * pageSize;

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@CustomerId", filter.CustomerId },
                {"@AgencyId", filter.AgencyId },
                {"@Keyword", filter.Keyword },
                {"@CreatedBy", filter.CreatedBy },
                {"@Status", filter.Status },
                {"@Offset", offset},
                {"@PageSize", pageSize},
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    using (var reader = MsSqlHelper.ExecuteReader(conn, CommandType.StoredProcedure, sqlCmd, parms))
                    {
                        listData = new List<IdentityOffer>();
                        while (reader.Read())
                        {
                            //var r = ExtractOfferData(reader);
                            var r = new IdentityOffer();
                            r.OfferRootId = Utils.ConvertToInt32(reader["OfferRootId"]);
                            r.Id = Utils.ConvertToInt32(reader["Id"]);

                            if (reader.HasColumn("TotalCount"))
                                r.TotalCount = Utils.ConvertToInt32(reader["TotalCount"]);

                            listData.Add(r);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return listData;
        }

        #endregion

        #region Quote

        public static IdentityOfferQuoteExport ExtractOfferQuoteExportData(IDataReader reader)
        {
            var record = new IdentityOfferQuoteExport();

            record.Id = Utils.ConvertToInt32(reader["Id"]);
            record.OfferRootId = Utils.ConvertToInt32(reader["OfferRootId"]);
            record.OfferId = Utils.ConvertToInt32(reader["OfferId"]);
            record.PackIdx = Utils.ConvertToInt32(reader["PackIdx"]);
            record.Code = reader["Code"].ToString();
            record.Name = reader["Name"].ToString();
            record.PdfPath = reader["PdfPath"].ToString();
            record.CreatedBy = Utils.ConvertToInt32(reader["CreatedBy"]);
            record.CreatedDate = reader["CreatedDate"] == DBNull.Value ? null : (DateTime?)reader["CreatedDate"];

            return record;
        }

        public List<IdentityOfferQuoteExport> GetQuoteExportByPage(IdentityOfferQuoteExport filter, int currentPage, int pageSize)
        {
            //Common syntax           
            var sqlCmd = @"OfferQuoteExport_GetByPage";
            List<IdentityOfferQuoteExport> listData = null;

            //For paging 
            int offset = (currentPage - 1) * pageSize;

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@OfferId", filter.OfferId },
                {"@CreatedBy", filter.CreatedBy },
                {"@Code", filter.Code },
                {"@Keyword", filter.Keyword },
                {"@Offset", offset},
                {"@PageSize", pageSize},
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    using (var reader = MsSqlHelper.ExecuteReader(conn, CommandType.StoredProcedure, sqlCmd, parms))
                    {
                        listData = new List<IdentityOfferQuoteExport>();
                        while (reader.Read())
                        {
                            var r = ExtractOfferQuoteExportData(reader);

                            listData.Add(r);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return listData;
        }

        public List<IdentityOfferQuoteExport> GetQuoteExportList(IdentityOfferQuoteExport filter)
        {
            //Common syntax           
            var sqlCmd = @"OfferQuoteExport_GetList";
            List<IdentityOfferQuoteExport> listData = null;

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@OfferId", filter.OfferId },
                {"@PackIdx", filter.PackIdx },
                {"@CreatedBy", filter.CreatedBy },
                {"@Code", filter.Code },
                {"@Keyword", filter.Keyword }
            };

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    using (var reader = MsSqlHelper.ExecuteReader(conn, CommandType.StoredProcedure, sqlCmd, parms))
                    {
                        listData = new List<IdentityOfferQuoteExport>();
                        while (reader.Read())
                        {
                            var r = ExtractOfferQuoteExportData(reader);

                            listData.Add(r);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return listData;
        }

        public int QuoteExportUpsert(IdentityOfferQuoteExport identity)
        {
            var returnId = 0;

            //Common syntax
            var sqlCmd = @"OfferQuoteExport_Update";

            //For parms
            var parms = new Dictionary<string, object>
            {
                {"@Id", identity.Id},
                {"@OfferRootId", identity.OfferRootId},
                {"@OfferId", identity.OfferId },
                {"@PackIdx", identity.PackIdx },
                {"@Code", identity.Code },
                {"@Name", identity.Name},
                {"@PdfPath", identity.PdfPath},
                {"@CreatedBy", identity.CreatedBy }
            };

            try
            {
                SqlConnection conn = new SqlConnection(_conStr);
                conn.Open();

                SqlTransaction trans = conn.BeginTransaction("OfferQuoteExport_Update");

                try
                {
                    var returnObj = MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, sqlCmd, parms).ExecuteScalar();

                    returnId = Utils.ConvertToInt32(returnObj);

                    trans.Commit();
                }
                catch (Exception tranEx)
                {
                    trans.Rollback();

                    throw tranEx;
                }

                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return returnId;
        }

        #endregion

        #region Import

        public int ImportData(IdentityOfferImport info)
        {
            var returnId = 0;

            //Common syntax           
            var sqlCmd = @"ImportData";

            try
            {
                if (info != null)
                {
                    _db.ChangeTracker.Clear();

                    info.CreatedDate = DateTime.Now;

                    _db.tbl_offer_import.Add(info);
                    _db.SaveChanges();

                    if (info.Id > 0)
                    {
                        if (info.Items.HasData())
                        {
                            foreach (var item in info.Items)
                            {
                                item.OfferImportId = info.Id;
                                item.ProjectId = info.ProjectId;
                                item.CustomerId = info.CustomerId;

                                _db.tbl_offer_import_item.Add(item);
                                _db.SaveChanges();

                                if (item.Id > 0)
                                {
                                    ImportSubItems(info, item.Id, item.SubItems);
                                }
                            }
                        }
                    }
                }

                returnId = info.Id;
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return returnId;
        }


        public List<IdentityOfferImport> GetImportDataByPage(IdentityOfferImport filter, int currentPage, int pageSize)
        {
            //Common syntax           
            var sqlCmd = @"Offer_GetImportData";
            List<IdentityOfferImport> listData = new List<IdentityOfferImport>();

            //For paging 
            int offset = (currentPage - 1) * pageSize;

            //For parameters
            var parameters = new DynamicParameters();
            parameters.Add("Keyword", filter.Keyword);
            parameters.Add("Offset", offset);
            parameters.Add("PageSize", pageSize);
            parameters.Add("ProjectId", filter.ProjectId);

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var dt = SqlMapper.QueryMultipleAsync(conn, sqlCmd, parameters, commandType: CommandType.StoredProcedure).Result;
                    if (!dt.IsConsumed)
                    {
                        listData = dt.Read<IdentityOfferImport>().ToList();
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return listData;
        }

        public IdentityOfferImport GetImportDataById(int id)
        {
            IdentityOfferImport result = null;

            //Common syntax           
            var sqlCmd = @"Offer_GetImportDataById";

            //For parameters
            var parameters = new DynamicParameters();
            parameters.Add("Id", id);

            try
            {
                result = _db.tbl_offer_import.Where(x => x.Id == id).FirstOrDefault();
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return result;
        }

        public List<IdentityOfferImport> GetImportDataByProject(int projectId)
        {
            List<IdentityOfferImport> result = null;

            //Common syntax           
            var sqlCmd = @"Offer_GetImportDataByProject";

            //For parameters
            var parameters = new DynamicParameters();
            parameters.Add("ProjectId", projectId);

            try
            {
                result = _db.tbl_offer_import.Where(x => x.ProjectId == projectId && x.Status != -99).OrderByDescending(x => x.Id).ToList();
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return result;
        }

        public IdentityOfferImportItem GetImportItemById(int id)
        {
            IdentityOfferImportItem result = null;

            //Common syntax           
            var sqlCmd = @"Offer_GetImportItemById";

            //For parameters
            var parameters = new DynamicParameters();
            parameters.Add("Id", id);

            try
            {
                result = _db.tbl_offer_import_item.Where(x => x.Id == id).FirstOrDefault();
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return result;
        }

        public List<IdentityOfferImportItem> GetImportItemsByData(int dataId)
        {
            //Common syntax           
            var sqlCmd = @"Offer_GetImportItemsByData";
            List<IdentityOfferImportItem> listData = new List<IdentityOfferImportItem>();

            //For parameters
            var parameters = new DynamicParameters();
            parameters.Add("Id", dataId);

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var dt = SqlMapper.QueryMultipleAsync(conn, sqlCmd, parameters, commandType: CommandType.StoredProcedure).Result;
                    if (!dt.IsConsumed)
                    {
                        listData = dt.Read<IdentityOfferImportItem>().ToList();
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return listData;
        }

        public bool UpdateImportItemType(int id, int type)
        {
            //Common syntax           
            var sqlCmd = @"Offer_UpdateImportItemType";

            //For parameters
            var parameters = new DynamicParameters();
            parameters.Add("Id", id);
            parameters.Add("Type", type);

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var rs = SqlMapper.QueryAsync(conn, sqlCmd, parameters, commandType: CommandType.StoredProcedure).Result;
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public bool UpdateImportItemDisplayName(int id, string displayName)
        {
            //Common syntax           
            var sqlCmd = @"Offer_UpdateImportItemDisplayName";

            //For parameters
            var parameters = new DynamicParameters();
            parameters.Add("Id", id);
            parameters.Add("DisplayName", displayName);

            try
            {
                using (var conn = new SqlConnection(_conStr))
                {
                    var rs = SqlMapper.QueryAsync(conn, sqlCmd, parameters, commandType: CommandType.StoredProcedure).Result;
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        public bool DeleteImportData(int id)
        {
            string transName = "Offer_DeleteImp";

            //For parameters
            var parms = new Dictionary<string, object>
            {
                {"@Id", id}
            };

            try
            {
                SqlConnection conn = new SqlConnection(_conStr);
                conn.Open();

                SqlTransaction trans = conn.BeginTransaction(transName);

                try
                {
                    MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, "Offer_DeleteImportData", parms).ExecuteNonQuery();

                    MsSqlHelper.TransactionInitStoreProcedureCommand(conn, trans, "Offer_DeleteImportItem", parms).ExecuteNonQuery();


                    trans.Commit();
                }
                catch (Exception tranEx)
                {
                    trans.Rollback();

                    throw tranEx;
                }

                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", transName, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return true;
        }

        private void ImportSubItems(IdentityOfferImport info, int parentId, List<IdentityOfferImportItem> subItems)
        {
            if (subItems.HasData())
            {
                foreach (var st in subItems)
                {
                    st.ParentId = parentId;
                    st.OfferImportId = info.Id;
                    st.ProjectId = info.ProjectId;
                    st.CustomerId = info.CustomerId;

                    _db.tbl_offer_import_item.Add(st);
                    _db.SaveChanges();

                    if (st.Id > 0)
                    {
                        if (st.SubItems.HasData())
                        {
                            ImportSubItems(info, st.Id, st.SubItems);
                        }
                    }
                }
            }
        }

        #endregion

        #region VE
        public int ApplyVE(IdentityVE info)
        {
            var sqlCmd = @"Offer_ApplyVE";
            var returnId = 0;
            string[] sumKeyword = { "合計", "中計", "小計", "仕切", "値引" };

            try
            {
                var affectedGrpIds = new List<int>();
                var currentOffer = _db.tbl_offer.Where(x => x.Id == info.SourceId).FirstOrDefault();

                if (currentOffer != null)
                {
                    if (!currentOffer.TotalPrice.HasValue)
                    {
                        currentOffer.TotalPrice = 0;
                    }

                    if (!currentOffer.CostPrice.HasValue)
                    {
                        currentOffer.CostPrice = 0;
                    }

                    var listMt = _db.tbl_offer_material.Where(x => x.OfferId == info.SourceId).ToList();

                    if (listMt.HasData() && info.Items.HasData())
                    {
                        _db.ChangeTracker.Clear();

                        foreach (var veItem in info.Items)
                        {
                            var updateItem = listMt.Find(x => x.Id == veItem.SourceId);

                            if (updateItem != null)
                            {
                                affectedGrpIds.Add(updateItem.GroupId);

                                if (!updateItem.TotalPrice.HasValue)
                                {
                                    updateItem.TotalPrice = 0;
                                }

                                currentOffer.TotalPrice -= updateItem.TotalPrice.GetValueOrDefault();
                                currentOffer.CostPrice -= updateItem.TotalCostPrice.GetValueOrDefault();

                                VEItemToMaterial(veItem, updateItem);

                                currentOffer.TotalPrice += updateItem.TotalPrice.GetValueOrDefault();
                                currentOffer.CostPrice += updateItem.TotalCostPrice.GetValueOrDefault();

                                _db.tbl_offer_material.Update(updateItem);
                                _db.SaveChanges();
                            }
                        }

                        affectedGrpIds = affectedGrpIds.Distinct().ToList();

                        if (affectedGrpIds.HasData())
                        {
                            foreach (var groupId in affectedGrpIds)
                            {
                                var currentGrp = _db.tbl_offer_group.Where(x => x.Id == groupId).FirstOrDefault();
                                if (currentGrp != null)
                                {
                                    var grpItems = listMt.Where(x => x.GroupId == currentGrp.Id && x.Type != 1 && !sumKeyword.Contains(x.ConstructionItem)).ToList();

                                    if (grpItems.HasData())
                                    {
                                        currentGrp.TotalPrice = grpItems.Sum(x => x.TotalPrice);

                                        _db.tbl_offer_group.Update(currentGrp);
                                        _db.SaveChanges();
                                    }
                                }
                            }
                        }

                        //update profit
                        currentOffer.GrossProfit = currentOffer.OfferPrice.GetValueOrDefault() - currentOffer.CostPrice.GetValueOrDefault();
                        if (currentOffer.CostPrice.GetValueOrDefault() != 0)
                        {
                            currentOffer.GrossProfitRatio = currentOffer.OfferPrice.GetValueOrDefault() / currentOffer.CostPrice.GetValueOrDefault();
                        } 
                        else
                        {
                            currentOffer.GrossProfitRatio = null;
                        }

                        _db.tbl_offer.Update(currentOffer);
                        _db.SaveChanges();

                        returnId = info.Id;
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", sqlCmd, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }

            return returnId;
        }

        #endregion

        #region AutoSave
        public IdentityOffer UpdateItems(IdentityOffer identity)
        {
            //Common syntax           
            var sqlCmd = @"Offer_UpdateItems";
            IdentityOffer returnObj = null;
            string trackName = "update offer";

            try
            {
                var currentOffer = _db.tbl_offer.Where(x => x.Id == identity.Id).FirstOrDefault();

                if (currentOffer != null)
                {
                    //update offer
                    currentOffer.TotalPrice = identity.TotalPrice;
                    currentOffer.OfferPrice = identity.OfferPrice;
                    currentOffer.CostPrice = identity.CostPrice;
                    currentOffer.GrossProfit = identity.GrossProfit;
                    currentOffer.GrossProfitRatio = identity.GrossProfitRatio;

                    _db.ChangeTracker.Clear();

                    _db.tbl_offer.Update(currentOffer);
                    _db.SaveChanges();

                    if (identity.Groups.HasData())
                    {
                        trackName = "update group";
                        _db.ChangeTracker.Clear();

                        foreach (var group in identity.Groups)
                        {
                            if (group.Id >= 0)
                            {
                                _db.tbl_offer_group.Update(group);
                                _db.SaveChanges();
                            }
                        }
                    }

                    if (identity.DetailLines.HasData())
                    {
                        trackName = "update material";
                        _db.ChangeTracker.Clear();

                        foreach (var dt in identity.DetailLines)
                        {
                            var itemId = dt.Id;

                            var currentItem = _db.tbl_offer_material.Where(x => x.Id == dt.Id).FirstOrDefault();
                            if (currentItem != null)
                            {
                                var childMtStr = currentItem.ChildMaterialStr;
                                currentItem = dt;
                                currentItem.Id = itemId;
                                currentItem.ChildMaterialStr = childMtStr;

                                _db.tbl_offer_material.Update(currentItem);
                                _db.SaveChanges();
                            }
                            else
                            {
                                dt.Id = 0;
                                _db.tbl_offer_material.Update(dt);
                                _db.SaveChanges();
                            }
                        }
                    }

                    if (identity.DeleteLines.HasData())
                    {
                        trackName = "delete material";
                        _db.ChangeTracker.Clear();

                        foreach (var del in identity.DeleteLines)
                        {
                            if (del.Type == 1)
                            {
                                var currentGrp = _db.tbl_offer_group.Where(x => x.Id == del.GroupId).FirstOrDefault();
                                if (currentGrp != null)
                                {
                                    _db.tbl_offer_group.Remove(currentGrp);
                                    _db.SaveChanges();
                                }
                            }

                            var existed = _db.tbl_offer_material.Where(x => x.Id == del.Id).FirstOrDefault();
                            if (existed != null)
                            {
                                _db.tbl_offer_material.Remove(existed);
                                _db.SaveChanges();
                            }
                        }
                    }
                }

                returnObj = identity;
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0} during {1}. Error: {2}. Item: {3}", sqlCmd, trackName, (ex.InnerException != null ? ex.InnerException.Message : ex.Message), identity.Id);
                returnObj = null;
                throw new CustomSQLException(strError);
            }

            return returnObj;
        }

        public IdentityOffer RemoveItems(IdentityOffer identity)
        {
            //Common syntax           
            var sqlCmd = @"Offer_RemoveItems";
            string trackName = "get current item";
            IdentityOffer returnObj = new IdentityOffer();

            try
            {
                var updateItems = new List<IdentityOfferMaterial>();

                if (identity.DetailLines.HasData())
                {
                    foreach (var item in identity.DetailLines)
                    {
                        var currentItem = _db.tbl_offer_material.Where(x => x.Id == item.Id).FirstOrDefault();
                        if (currentItem != null)
                        {
                            currentItem.GroupId = 0;
                            updateItems.Add(currentItem);
                        }
                    }
                }

                if (updateItems.HasData())
                {
                    trackName = "update item";
                    _db.ChangeTracker.Clear();

                    _db.tbl_offer_material.UpdateRange(updateItems);
                    _db.SaveChanges();

                    if (identity.Groups.HasData())
                    {
                        _db.tbl_offer_group.UpdateRange(identity.Groups);
                        _db.SaveChanges();
                    }
                }

                returnObj = identity;
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0} during {1}. Error: {2}", sqlCmd, trackName, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                returnObj = null;
                throw new CustomSQLException(strError);
            }

            return returnObj;
        }

        public IdentityOffer UpdateGroups(IdentityOffer identity)
        {
            //Common syntax           
            var sqlCmd = @"Offer_UpdateGroups";
            IdentityOffer returnObj = new IdentityOffer();
            string trackName = "update offer";

            try
            {
                var materials = new List<IdentityOfferMaterial>();
                var currentOffer = _db.tbl_offer.Where(x => x.Id == identity.Id).FirstOrDefault();

                if (currentOffer != null)
                {
                    _db.ChangeTracker.Clear();

                    //update offer
                    currentOffer.TotalPrice = identity.TotalPrice;
                    currentOffer.DisPr = identity.DisPr;
                    currentOffer.OfferPrice = identity.OfferPrice;

                    currentOffer.CostPrice = identity.CostPrice;
                    currentOffer.GrossProfit = identity.GrossProfit;
                    currentOffer.GrossProfitRatio = identity.GrossProfitRatio;

                    _db.tbl_offer.Update(currentOffer);
                    _db.SaveChanges();

                    //update nebiki
                    var disMs = _db.tbl_offer_material.Where(x => x.OfferId == currentOffer.Id && x.ConstructionItem == "値引").ToList();
                    if (disMs.HasData())
                    {
                        trackName = "update nebiki";
                        _db.ChangeTracker.Clear();

                        foreach (var item in disMs)
                        {
                            item.TotalPrice = currentOffer.DisPr;
                            _db.tbl_offer_material.Update(item);

                            _db.SaveChanges();
                        }
                    }

                    if (identity.Groups.HasData())
                    {
                        trackName = "update group";
                        _db.ChangeTracker.Clear();

                        var needUpdateItems = false;

                        foreach (var grp in identity.Groups)
                        {
                            var isGrpExist = _db.tbl_offer_group.Any(x => x.Id == grp.Id);
                            if (isGrpExist)
                            {
                                _db.tbl_offer_group.Update(grp);
                                _db.SaveChanges();
                            }
                            else
                            {
                                grp.Id = 0;
                                _db.tbl_offer_group.Update(grp);
                                _db.SaveChanges();
                            }

                            if (grp.Materials.HasData())
                            {
                                trackName = "update material";
                                _db.ChangeTracker.Clear();

                                if (!needUpdateItems)
                                {
                                    if (grp.NeedUpdateItems)
                                    {
                                        needUpdateItems = true;
                                    }
                                }

                                foreach (var mat in grp.Materials)
                                {
                                    if (mat.Id > 0)
                                    {
                                        var currentMt = _db.tbl_offer_material.Where(x => x.Id == mat.Id).FirstOrDefault();

                                        if (currentMt != null)
                                        {
                                            currentMt.GroupId = grp.Id;
                                            currentMt.Type = mat.Type;

                                            if (mat.SubMaterials.HasData())
                                            {
                                                currentMt.SubMaterialStr = JsonConvert.SerializeObject(mat.SubMaterials);
                                            }
                                            else
                                            {
                                                currentMt.SubMaterialStr = null;
                                            }

                                            if (currentMt.ConstructionItem == "仕切")
                                            {
                                                currentMt.TotalPrice = identity.OfferPrice;
                                            }
                                            else if (currentMt.ConstructionItem == "値引")
                                            {
                                                currentMt.TotalPrice = identity.OfferPrice.GetValueOrDefault() - identity.TotalPrice.GetValueOrDefault();
                                            }

                                            _db.tbl_offer_material.Update(currentMt);
                                            _db.SaveChanges();
                                        }
                                    }
                                    else
                                    {
                                        mat.Id = 0;
                                        mat.OfferRootId = identity.OfferRootId;
                                        mat.OfferId = identity.Id;
                                        mat.GroupId = grp.Id;
                                        mat.ProjectId = identity.ProjectId;

                                        _db.tbl_offer_material.Update(mat);
                                        _db.SaveChanges();
                                    }
                                }
                            }
                        }

                        if (needUpdateItems)
                        {
                            string[] filterArr = { "仕切", "値引" };

                            var currentMts = _db.tbl_offer_material.Where(x => x.OfferId == identity.Id && filterArr.Contains(x.ConstructionItem)).ToList();

                            if (currentMts.HasData())
                            {
                                trackName = "update material nebiki";
                                _db.ChangeTracker.Clear();

                                foreach (var mat in currentMts)
                                {
                                    if (mat.ConstructionItem == "仕切")
                                    {
                                        mat.TotalPrice = identity.OfferPrice;
                                        _db.tbl_offer_material.Update(mat);
                                        _db.SaveChanges();
                                    }
                                    else if (mat.ConstructionItem == "値引")
                                    {
                                        mat.TotalPrice = identity.OfferPrice.GetValueOrDefault() - identity.TotalPrice.GetValueOrDefault();
                                        _db.tbl_offer_material.Update(mat);
                                        _db.SaveChanges();
                                    }
                                }
                            }
                        }
                    }

                    if (identity.RemoveGroups.HasData())
                    {
                        trackName = "remove group";
                        _db.ChangeTracker.Clear();

                        foreach (var group in identity.RemoveGroups)
                        {
                            var currentGrp = _db.tbl_offer_group.Where(x => x.Id == group.Id).FirstOrDefault();
                            if (currentGrp != null)
                            {
                                _db.tbl_offer_group.Remove(currentGrp);
                                _db.SaveChanges();
                            }
                        }
                    }

                    if (identity.Groups.HasData() && materials.HasData())
                    {
                        foreach (var gr in identity.Groups)
                        {
                            gr.Materials = materials.Where(x => x.GroupId == gr.Id).ToList();
                        }
                    }

                    returnObj.Groups = identity.Groups;
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0} during {1}. Error: {2}. OfferId: {3}", sqlCmd, trackName, (ex.InnerException != null ? ex.InnerException.Message : ex.Message), identity.Id);
                returnObj = null;
                throw new CustomSQLException(strError);
            }

            return returnObj;
        }

        public IdentityOffer RemoveGroups(IdentityOffer identity)
        {
            //Common syntax           
            var sqlCmd = @"Offer_RemoveGroups";
            string trackName = "update offer";
            IdentityOffer returnObj = new IdentityOffer();

            try
            {
                var currentOffer = _db.tbl_offer.Where(x => x.Id == identity.Id).FirstOrDefault();
                var currentGroups = _db.tbl_offer_group.Where(x => x.OfferId == identity.Id).ToList();

                _db.ChangeTracker.Clear();

                //update offer
                if (currentOffer != null)
                {
                    currentOffer.TotalPrice = identity.TotalPrice;
                    currentOffer.OfferPrice = identity.OfferPrice;

                    currentOffer.CostPrice = identity.CostPrice;
                    currentOffer.GrossProfit = identity.GrossProfit;
                    currentOffer.GrossProfitRatio = identity.GrossProfitRatio;

                    _db.tbl_offer.Update(currentOffer);
                    _db.SaveChanges();
                }

                //update remain groups
                if (identity.Groups.HasData())
                {
                    trackName = "update remain group";
                    _db.ChangeTracker.Clear();

                    foreach (var group in identity.Groups)
                    {
                        var currentGrp = _db.tbl_offer_group.Where(x => x.Id == group.Id).FirstOrDefault();
                        if (currentGrp != null)
                        {
                            currentGrp.SortOrder = group.SortOrder;
                            currentGrp.TotalPrice = group.TotalPrice;

                            _db.tbl_offer_group.Update(currentGrp);
                            _db.SaveChanges();
                        }
                    }
                }

                //change type and groupId of item in remove group, then remove that group
                if (identity.RemoveGroups.HasData())
                {
                    trackName = "remove group";
                    _db.ChangeTracker.Clear();

                    foreach (var rmvGrp in identity.RemoveGroups)
                    {
                        var groupItems = _db.tbl_offer_material.Where(x => x.GroupId == rmvGrp.Id).ToList();

                        if (groupItems.HasData())
                        {
                            foreach (var item in groupItems)
                            {
                                item.GroupId = rmvGrp.NewId;
                                item.Type = 0;

                                _db.tbl_offer_material.Update(item);
                                _db.SaveChanges();
                            }
                        }

                        _db.tbl_offer_group.Remove(rmvGrp);
                        _db.SaveChanges();
                    }

                    //update material if any (for sub-mt in edge cases)
                    if (identity.DetailLines.HasData())
                    {
                        trackName = "update material";
                        _db.ChangeTracker.Clear();

                        foreach (var dt in identity.DetailLines)
                        {
                            var itemId = dt.Id;

                            var currentItem = _db.tbl_offer_material.Where(x => x.Id == dt.Id).FirstOrDefault();
                            if (currentItem != null)
                            {
                                var childMtStr = currentItem.ChildMaterialStr;
                                currentItem = dt;
                                currentItem.Id = itemId;
                                currentItem.ChildMaterialStr = childMtStr;

                                _db.tbl_offer_material.Update(currentItem);
                                _db.SaveChanges();
                            }
                            else
                            {
                                dt.Id = 0;
                                _db.tbl_offer_material.Update(dt);
                                _db.SaveChanges();
                            }
                        }
                    }

                    if (identity.DeleteLines.HasData())
                    {
                        trackName = "delete material";
                        _db.ChangeTracker.Clear();

                        foreach (var del in identity.DeleteLines)
                        {
                            var existed = _db.tbl_offer_material.Where(x => x.Id == del.Id).FirstOrDefault();
                            if (existed != null)
                            {
                                _db.tbl_offer_material.Remove(existed);
                                _db.SaveChanges();
                            }
                        }
                    }

                    returnObj = identity;
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0} during {1}. Error: {2}. OfferId: {3}", sqlCmd, trackName, (ex.InnerException != null ? ex.InnerException.Message : ex.Message), identity.Id);
                returnObj = null;
                throw new CustomSQLException(strError);
            }

            return returnObj;
        }

        public int DeleteGroups(IdentityOffer identity)
        {
            //Common syntax           
            var sqlCmd = @"Offer_DeleteGroups";
            string trackName = "update offer";
            var returnId = 0;

            try
            {
                var currentOffer = _db.tbl_offer.Where(x => x.Id == identity.Id).FirstOrDefault();

                if (currentOffer != null)
                {
                    currentOffer.TotalPrice = identity.TotalPrice;
                    currentOffer.OfferPrice = identity.OfferPrice;

                    currentOffer.CostPrice = identity.CostPrice;
                    currentOffer.GrossProfit = identity.GrossProfit;
                    currentOffer.GrossProfitRatio = identity.GrossProfitRatio;

                    var currentGroups = _db.tbl_offer_group.Where(x => x.OfferId == currentOffer.Id).ToList();

                    _db.ChangeTracker.Clear();

                    _db.tbl_offer.Update(currentOffer);
                    _db.SaveChanges();

                    //update groups
                    if (identity.Groups.HasData())
                    {
                        trackName = "update group";
                        _db.ChangeTracker.Clear();

                        _db.tbl_offer_group.UpdateRange(identity.Groups);
                        _db.SaveChanges();
                    }

                    //remove groups
                    var delItems = currentGroups.Where(x => identity.Groups == null || !identity.Groups.Any(y => y.Id == x.Id && y.Id > 0)).ToList();
                    if (delItems.HasData())
                    {
                        trackName = "remove group";
                        _db.ChangeTracker.Clear();

                        _db.tbl_offer_group.RemoveRange(delItems);
                        _db.SaveChanges();
                    }

                    returnId = identity.Id;
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0} during {1}. Error: {2}. OfferId: {3}", sqlCmd, trackName, (ex.InnerException != null ? ex.InnerException.Message : ex.Message), identity.Id);
                throw new CustomSQLException(strError);
            }

            return returnId;
        }

        public int UpdateSummary(IdentityOffer identity)
        {
            var returnId = 0;
            var sqlCmd = @"Offer_UpdateSummary";
            string trackName = "udpate offer";

            try
            {
                var currentOffer = _db.tbl_offer.Where(x => x.Id == identity.Id).FirstOrDefault();

                //update offer
                if (currentOffer != null)
                {
                    identity.TotalPrice = currentOffer.TotalPrice;
                    identity.OfferPrice = currentOffer.OfferPrice;

                    currentOffer.CostPrice = identity.CostPrice;
                    currentOffer.GrossProfit = identity.GrossProfit;
                    currentOffer.GrossProfitRatio = identity.GrossProfitRatio;

                    _db.ChangeTracker.Clear();

                    _db.tbl_offer.Update(identity);
                    _db.SaveChanges();

                    //update offer config
                    if (identity.Configs.HasData())
                    {
                        trackName = "update config";
                        _db.ChangeTracker.Clear();

                        foreach (var item in identity.Configs)
                        {
                            _db.tbl_worksheet_config.Update(item);
                            _db.SaveChanges();
                        }
                    }

                    //update offer root
                    var currentOfferRoot = _db.tbl_offer_root.Where(x => x.Id == currentOffer.OfferRootId).FirstOrDefault();
                    if (currentOfferRoot != null)
                    {
                        trackName = "update offer root";
                        _db.ChangeTracker.Clear();

                        currentOfferRoot.Name = identity.Name;
                        _db.tbl_offer_root.Update(currentOfferRoot);
                        _db.SaveChanges();
                    }

                    returnId = identity.Id;
                }

            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0} during {1}. Error: {2}. OfferId: {3}", sqlCmd, trackName, (ex.InnerException != null ? ex.InnerException.Message : ex.Message), identity.Id);
                throw new CustomSQLException(strError);
            }

            return returnId;
        }

        public int DeleteItems(IdentityOffer identity)
        {
            //Common syntax           
            var sqlCmd = @"Offer_DeleteItems";
            string trackName = "update offer";

            int returnId = 0;

            try
            {
                var currentOffer = _db.tbl_offer.Where(x => x.Id == identity.Id).FirstOrDefault();

                if (currentOffer != null)
                {
                    currentOffer.TotalPrice = identity.TotalPrice;
                    currentOffer.OfferPrice = identity.OfferPrice;

                    currentOffer.CostPrice = identity.CostPrice;
                    currentOffer.GrossProfit = identity.GrossProfit;
                    currentOffer.GrossProfitRatio = identity.GrossProfitRatio;

                    _db.ChangeTracker.Clear();

                    _db.tbl_offer.Update(currentOffer);
                    _db.SaveChanges();


                    if (identity.Groups.HasData())
                    {
                        trackName = "update group";
                        _db.ChangeTracker.Clear();

                        _db.tbl_offer_group.UpdateRange(identity.Groups);
                        _db.SaveChanges();
                    }

                    if (identity.RemoveGroups.HasData())
                    {
                        trackName = "remove group";
                        _db.ChangeTracker.Clear();

                        _db.tbl_offer_group.RemoveRange(identity.RemoveGroups);
                        _db.SaveChanges();
                    }

                    if (identity.DetailLines.HasData())
                    {
                        trackName = "update child material";
                        _db.ChangeTracker.Clear();

                        foreach (var dt in identity.DetailLines)
                        {
                            var itemId = dt.Id;
                            var currentItem = _db.tbl_offer_material.Where(x => x.Id == dt.Id).FirstOrDefault();
                            if (currentItem != null)
                            {
                                var childMtStr = currentItem.ChildMaterialStr;
                                currentItem = dt;
                                currentItem.Id = itemId;
                                currentItem.ChildMaterialStr = childMtStr;

                                _db.tbl_offer_material.Update(currentItem);
                                _db.SaveChanges();
                            }
                        }
                    }

                    if (identity.DeleteLines.HasData())
                    {
                        trackName = "delete line";
                        _db.ChangeTracker.Clear();

                        foreach (var dt in identity.DeleteLines)
                        {
                            var existed = _db.tbl_offer_material.Where(x => x.Id == dt.Id).FirstOrDefault();
                            if (existed != null)
                            {
                                _db.tbl_offer_material.Remove(existed);
                            }

                            _db.SaveChanges();
                        }
                    }
                }

                returnId = identity.Id;
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0} during: {1}. Error: {2}. OfferId: {3}", sqlCmd, trackName, (ex.InnerException != null ? ex.InnerException.Message : ex.Message), identity.Id);
                throw new CustomSQLException(strError);
            }

            return returnId;
        }

        #endregion

        #region Helper
        private void VEItemToMaterial(IdentityVEItem veItem, IdentityOfferMaterial item)
        {
            if (!string.IsNullOrEmpty(veItem.MaterialName))
            {
                item.MaterialName = veItem.MaterialName;
            }

            if (!string.IsNullOrEmpty(veItem.MakerIds))
            {
                item.MakerIds = veItem.MakerIds;
            }

            if (veItem.TotalPrice != 0 && veItem.TotalPrice.HasValue)
            {
                item.TotalPrice = veItem.TotalPrice;
            }

            if (veItem.TotalCostPrice != 0 && veItem.TotalCostPrice.HasValue)
            {
                item.TotalCostPrice = veItem.TotalCostPrice;
            }

            if (veItem.InstallPrice != 0 && veItem.InstallPrice.HasValue)
            {
                item.InstallPrice = veItem.InstallPrice;
            }

            if (veItem.UnitPrice != 0 && veItem.UnitPrice.HasValue)
            {
                item.UnitPrice = veItem.UnitPrice;
            }

            if (veItem.UnitCostPrice != 0 && veItem.UnitCostPrice.HasValue)
            {
                item.UnitCostPrice = veItem.UnitCostPrice;
            }
        }

        /// <summary>
        ///  (Do code của offer và offer root theo dạng: string.Format("MM{0}", EpochTime.GetIntDate(DateTime.UtcNow)))
        ///  Dùng để lấy lại created date cho những record đang không có
        /// </summary>
        /// <exception cref="CustomSQLException"></exception>
        public void SyncCreatedDate()
        {
            var transName = "Offer_SyncCreatedDate";
            try
            {
                var roots = _db.tbl_offer_root.Where(x => !string.IsNullOrWhiteSpace(x.Code) && x.CreatedDate == null).ToList();
                if (roots.HasData())
                {
                    var updateList = new List<IdentityOfferRoot>();
                    foreach (var root in roots)
                    {
                        var code = root.Code;
                        var epochDate = code.Replace("MM", "");
                        DateTime? createdDate = null;
                        try
                        {
                            createdDate = EpochTimeConverter.GetDateTime(Utils.ConvertToInt64(epochDate, 0));
                        }
                        catch (Exception e)
                        {
                            createdDate = null;
                        }
                        if (createdDate != null && createdDate.HasValue)
                        {
                            root.CreatedDate = createdDate;
                            updateList.Add(root);
                        }
                    }

                    if (updateList.HasData())
                    {
                        _db.ChangeTracker.Clear();
                        _db.tbl_offer_root.UpdateRange(updateList);
                        _db.SaveChanges();
                    }
                }

                var offers = _db.tbl_offer.Where(x => !string.IsNullOrWhiteSpace(x.Code) && x.CreatedDate == null).ToList();
                if (offers.HasData())
                {
                    var updateList = new List<IdentityOffer>();
                    foreach (var offer in offers)
                    {
                        var code = offer.Code;
                        var epochDate = code.Replace("MM", "");
                        DateTime? createdDate = null;
                        try
                        {
                            createdDate = EpochTimeConverter.GetDateTime(Utils.ConvertToInt64(epochDate, 0));
                        }
                        catch (Exception e)
                        {
                            createdDate = null;
                        }
                        if (createdDate != null && createdDate.HasValue)
                        {
                            offer.CreatedDate = createdDate;
                            updateList.Add(offer);
                        }
                    }

                    if (updateList.HasData())
                    {
                        _db.ChangeTracker.Clear();
                        _db.tbl_offer.UpdateRange(updateList);
                        _db.SaveChanges();
                    }
                }
            }
            catch (Exception ex)
            {
                var strError = string.Format("Failed to execute {0}. Error: {1}", transName, (ex.InnerException != null ? ex.InnerException.Message : ex.Message));
                throw new CustomSQLException(strError);
            }
        }

        #endregion
    }
}
